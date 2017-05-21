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
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
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
class KafkaObservableStream
    implements ObservableStream<KafkaStreamElement>, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaObservableStream.class);

  private final String name;
  private final String topic;
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final Executor executor;
  private final String[] bootstrapServers;
  private final Deserializer<Window> windowDeserializer;
  private final Deserializer<Object> payloadDeserializer;
  private final AtomicReference<List<Integer>> assignedPartitions;
  private final AtomicBoolean finish = new AtomicBoolean();
  private final Set<Integer> finishedPartitions;
  private final BlockingQueue<StreamObserver<KafkaStreamElement>> newObservers;
  private final AtomicBoolean signalAssignment = new AtomicBoolean(false);

  @SuppressWarnings("unchecked")
  KafkaObservableStream(
      Executor executor,
      String[] bootstrapServers,
      TopicSpec spec,
      String name) {

    this.name = name;
    this.topic = spec.getName();    
    this.executor = executor;
    this.bootstrapServers = bootstrapServers;
    this.windowDeserializer = spec.getWindowSerialization().deserializer();
    this.payloadDeserializer = spec.getPayloadSerialization().deserializer();
    this.assignedPartitions = new AtomicReference<>();
    this.newObservers = new LinkedBlockingQueue<>();
    this.finishedPartitions = Collections.synchronizedSet(new HashSet<>());
   
    this.consumer = createConsumer(name, bootstrapServers, topic);
    CountDownLatch assignmentLatch = new CountDownLatch(1);
    consumer.subscribe(Arrays.asList(topic), rebalanceListener(assignmentLatch));
    executor.execute(this::startObserving);
    try {
      assignmentLatch.await();
    } catch (InterruptedException ex) {
      consumer.close();
      Thread.currentThread().interrupt();
    }
    
  }

  private void startObserving() {
    LOG.info("Started to observe Kafka topic {}", topic);
    List<StreamObserver<KafkaStreamElement>> registered = new ArrayList<>();
    try {
      while (!Thread.currentThread().isInterrupted() && !finish.get()) {
        if (!newObservers.isEmpty()) {
          List<StreamObserver<KafkaStreamElement>> added = new ArrayList<>();
          newObservers.drainTo(added);
          added.forEach(o -> o.onAssign(assignedPartitions.get().size()));
          registered.addAll(added);
        }
        if (signalAssignment.get()) {
          signalAssignment.set(false);
          registered.forEach(o -> o.onAssign(assignedPartitions.get().size()));
        }
        ConsumerRecords<byte[], byte[]> polled = consumer.poll(100);
        for (ConsumerRecord<byte[], byte[]> r : polled) {
          Optional<KafkaStreamElement> parsed = deserialize(r.timestamp(), r.value());
          if (parsed.isPresent()) {
            KafkaStreamElement elem = parsed.get();
            if (!elem.isEndOfStream()) {
              registered.forEach(o -> o.onNext(
                  assignedPartitions.get().indexOf(r.partition()), elem));
            } else {
              finishedPartitions.add(r.partition());
              if (finishedPartitions.size() == assignedPartitions.get().size()) {
                finish.set(true);
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
      consumer.close();
      registered.forEach(o -> o.onCompleted());
    } catch (Throwable err) {
      registered.forEach(o -> o.onError(err));
    }
  }


  @Override
  public int size() {
    return consumer.partitionsFor(topic).size();
  }

  @Override
  public void observe(StreamObserver<KafkaStreamElement> observer) {
    newObservers.add(observer);
  }

  private KafkaConsumer<byte[], byte[]> createConsumer(
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
    
    return new KafkaConsumer<>(props);
  }

  private ConsumerRebalanceListener rebalanceListener(CountDownLatch latch) {
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
        finishedPartitions.clear();
        signalAssignment.set(true);
        latch.countDown();
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

  @Override
  public void close() {
    finish.set(true);
  }

}
