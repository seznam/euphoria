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

import cz.seznam.euphoria.inmem.operator.StreamElement;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Joiner;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

/**
 * A stream stored in Apache Kafka topic.
 */
public class KafkaObservableStream<T extends StreamElement<Object>>
    implements ObservableStream<T> {

  private final String topic;
  private final Executor executor;
  private final String[] bootstrapServers;
  private final Function<ConsumerRecord<byte[], byte[]>, T> deserializer;

  public KafkaObservableStream(
      Executor executor,
      String[] bootstrapServers,
      String topic,
      Function<ConsumerRecord<byte[], byte[]>, T> deserializer) {

    this.topic = topic;
    this.executor = executor;
    this.bootstrapServers = bootstrapServers;
    this.deserializer = deserializer;
  }

  @Override
  public void observe(String name, StreamObserver<T> observer) {
    executor.execute(() -> {
      KafkaConsumer<byte[], byte[]> consumer = createConsumer(name, bootstrapServers, topic);
      observer.onRegistered();
      try {
        boolean finished = false;
        while (!finished && !Thread.currentThread().isInterrupted()) {
          ConsumerRecords<byte[], byte[]> polled = consumer.poll(100);
          for (ConsumerRecord<byte[], byte[]> r : polled) {
            T elem = deserializer.apply(r);
            if (!elem.isEndOfStream()) {
              observer.onNext(r.partition(), elem);
            } else {
              finished = true;
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
    
    return new KafkaConsumer<>(props);
  }

}
