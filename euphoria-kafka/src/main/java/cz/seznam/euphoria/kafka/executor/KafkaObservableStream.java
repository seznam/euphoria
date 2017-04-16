/*
 * Copyright 2016-2017 Seznam.cz, a.s..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.kafka.executor;

import cz.seznam.euphoria.shaded.guava.com.google.common.base.Joiner;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;

/**
 * A stream stored in Apache Kafka topic.
 */
public class KafkaObservableStream
    implements ObservableStream<KafkaStreamElement> {

  private final String topic;
  private final Executor executor;
  private final String[] bootstrapServers;
  private final Function<byte[], Object> deserializer;
  private final AtomicReference<List<Integer>> assignedPartitions;

  public KafkaObservableStream(
      Executor executor,
      String[] bootstrapServers,
      String topic,
      Function<byte[], Object> deserializer) {

    this.topic = topic;
    this.executor = executor;
    this.bootstrapServers = bootstrapServers;
    this.deserializer = deserializer;
    this.assignedPartitions = new AtomicReference<>();
  }

  @Override
  public void observe(String name, StreamObserver<KafkaStreamElement> observer) {
    executor.execute(() -> {
      KafkaConsumer<byte[], byte[]> consumer;

      consumer = createConsumer(name, bootstrapServers, topic);
      observer.onRegistered();
      try {
        boolean finished = false;
        Set<Integer> finishedPartitions = new HashSet<>();
        while (!finished && !Thread.currentThread().isInterrupted()) {
          ConsumerRecords<byte[], byte[]> polled = consumer.poll(100);
          for (ConsumerRecord<byte[], byte[]> r : polled) {
            if (r.value() != null) {
              // FIXME: window serialization
              Object elem = deserializer.apply(r.value());
              observer.onNext(
                  r.partition(),
                  KafkaStreamElement.FACTORY.data(elem, null, r.timestamp()));
            } else {
              finishedPartitions.add(r.partition());
              if (finishedPartitions.size() == assignedPartitions.get().size()) {
                finished = true;
              }
            }
            consumer.commitAsync();
          }
        }
        observer.onCompleted();
      } catch (Throwable thrwbl) {
        observer.onError(thrwbl);
      }
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

}
