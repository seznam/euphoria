/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cz.seznam.euphoria.kafka.executor;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.kafka.executor.io.Serde.Deserializer;
import cz.seznam.euphoria.kafka.executor.io.TopicSpec;
import cz.seznam.euphoria.kafka.executor.proto.Serialization;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Joiner;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stream stored in Apache Kafka topic.
 */
public class KafkaObservableStream
    implements ObservableStream<KafkaStreamElement> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaObservableStream.class);

  private final String topic;
  private final Executor executor;
  private final String[] bootstrapServers;
  private final Deserializer<Window> windowDeserializer;
  private final Deserializer<Object> payloadDeserializer;
  private final AtomicReference<List<Integer>> assignedPartitions;

  KafkaObservableStream(
      Executor executor,
      String[] bootstrapServers,
      TopicSpec spec) {

    this.topic = spec.getName();
    this.executor = executor;
    this.bootstrapServers = bootstrapServers;
    this.windowDeserializer = spec.getWindowSerialization().deserializer();
    this.payloadDeserializer = spec.getPayloadSerialization().deserializer();
    this.assignedPartitions = new AtomicReference<>();
  }

  @Override
  public void observe(
      String name, StreamObserver<KafkaStreamElement> observer) {
    
    executor.execute(() -> {
      KafkaConsumer<byte[], byte[]> consumer = createConsumer(
          name, bootstrapServers, topic);
      boolean first = true;
      try {
        boolean finished = false;
        Set<Integer> finishedPartitions = new HashSet<>();
        while (!finished && !Thread.currentThread().isInterrupted()) {
          ConsumerRecords<byte[], byte[]> polled = consumer.poll(100);
          if (first) {
            LOG.info("Started to consume topic {}", topic);
            observer.onRegistered();
            first = false;
          }
          for (ConsumerRecord<byte[], byte[]> r : polled) {
            Optional<KafkaStreamElement> parsed = deserialize(r.timestamp(), r.value());
            if (parsed.isPresent()) {
              KafkaStreamElement elem = parsed.get();
              if (!elem.isEndOfStream()) {
                observer.onNext(
                    r.partition(),
                    elem);
              } else {
                finishedPartitions.add(r.partition());
                if (finishedPartitions.size() == assignedPartitions.get().size()) {
                  finished = true;
                }
              }
            }            
          }
          if (!polled.isEmpty()) {
            // FIXME: this doesn't have exactly once-semantics
            // need to start working with the state checkpointing
            // and CHECKPOINT and RESET_TO_CHECKPOINT messages
            // sent across the whole computation
            consumer.commitAsync();
          }
        }
        observer.onCompleted();
      } catch (Throwable thrwbl) {
        LOG.error("Error reading stream {}", name, thrwbl);
        observer.onError(thrwbl);
      }
      consumer.close();
    });
  }

  protected KafkaConsumer<byte[], byte[]> createConsumer(
      String name, String[] bootstrapServers, String topic) {

    Properties props = new Properties();
    props.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        Joiner.on(',').join(bootstrapServers));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, name);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        Serdes.ByteArray().deserializer().getClass());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        Serdes.ByteArray().deserializer().getClass());
    
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(topic), rebalanceListener());
    return consumer;
  }

  @Override
  public int size() {
    return assignedPartitions.get().size();
  }

  private ConsumerRebalanceListener rebalanceListener() {
    return new ConsumerRebalanceListener() {

      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> clctn) {
        // nop
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        assignedPartitions.set(partitions
            .stream().map(TopicPartition::partition)
            .collect(Collectors.toList()));
      }

    };
  }

  private Optional<KafkaStreamElement> deserialize(long timestamp, byte[] value) {
    try {
      Serialization.Element parsed = Serialization.Element.parseFrom(value);
      switch (parsed.getType()) {
        case ELEMENT:
          return Optional.of(KafkaStreamElement.FACTORY.data(
              payloadDeserializer.apply(parsed.getPayload().toByteArray()),
              windowDeserializer.apply(parsed.getWindow().toByteArray()),
              timestamp));
        case END_OF_STREAM:
          return Optional.of(KafkaStreamElement.FACTORY.endOfStream());
        case WATERMARK:
          return Optional.of(KafkaStreamElement.FACTORY.watermark(timestamp));
        case WINDOW_TRIGGER:
          return Optional.of(KafkaStreamElement.FACTORY.windowTrigger(
              windowDeserializer.apply(parsed.getWindow().toByteArray()),
              timestamp));
        default:
          LOG.warn(
              "Unknown streaming element {}",
              TextFormat.shortDebugString(parsed));
      }
    } catch (InvalidProtocolBufferException ex) {
      LOG.warn("Cannot deserialize input data", ex);
    }
    return Optional.empty();
  }

}
