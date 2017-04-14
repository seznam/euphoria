/*
 * Copyright 2017 Seznam.cz, a.s..
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

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.operator.test.AllOperatorsSuite;
import cz.seznam.euphoria.operator.test.FlatMapTest;
import cz.seznam.euphoria.operator.test.junit.ExecutorEnvironment;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.runners.Suite;

/**
 * Use operator test kit to test the {@code KafkaExecutor} compliance with
 * Euphoria operators semantics.
 */
@Suite.SuiteClasses({
    FlatMapTest.class
})
public class KafkaOperatorsTest extends AllOperatorsSuite {

  @Override
  public ExecutorEnvironment newExecutorEnvironment() throws Exception {
    return new ExecutorEnvironment() {
      ExecutorService service = Executors.newCachedThreadPool();
      @Override
      public Executor getExecutor() {
        return new KafkaExecutor(service, new String[] { }) {

          @Override
          ObservableStream<?> kafkaObservableStream(
              Dataset<?> dataset,
              Function<ConsumerRecord<byte[], byte[]>, KafkaStreamElement> deserializer) {

            // FIXME
            return null;
          }

        };
      }

      @Override
      public void shutdown() throws Exception {
        service.shutdownNow();
      }

    };
  }

}
