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

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.util.Pair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * A {@code KafkaExecutor} that actually doesn't use kafka, but uses
 * {@code BlockingQueue} for all data pipelines. This makes this
 * executor actually only sort of equivalent to {@code InMemExecutor}.
 */
class TestKafkaExecutor extends KafkaExecutor {

  final Map<String, ObservableStream<KafkaStreamElement>> topicQueues = new HashMap<>();

  TestKafkaExecutor(ExecutorService executor) {
    super(executor, new String[] { });
  }

  @Override
  ObservableStream<KafkaStreamElement> kafkaObservableStream(
      Flow flow,
      Dataset<?> input,
      Function<byte[], KafkaStreamElement> deserializer) {

    String topic = topicGenerator.apply(flow, input);
    return Objects.requireNonNull(
        topicQueues.get(topic),
        "Cannot find stream  " + topic);
  }

  @Override
  OutputWriter outputWriter(Flow flow, Dataset<?> output) {
    List<Pair<BlockingQueue<KafkaStreamElement>, Integer>> queues;
    queues = createOutputQueues(output.getNumPartitions());
    String topic = topicGenerator.apply(flow, output);
    topicQueues.put(
        topic,
        BlockingQueueObservableStream.wrap(getExecutor(), topic, queues));
    
    return new OutputWriter() {

      @Override
      public int numPartitions() {
        return output.getNumPartitions();
      }
      
      @Override
      public void write(
          KafkaStreamElement elem, int source, int target, Callback callback) {
        try {
          queues.get(target).getFirst().put(new KafkaStreamElement(
              elem.getElement(),
              elem.getWindow(),
              elem.getTimestamp(),
              elem.type,
              source));
          callback.apply(true, null);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          callback.apply(false, ex);
        } catch (Exception ex) {
          callback.apply(false, ex);
        }
      }

      @Override
      public void close() {
        
      }

    };
    
  }

  @Override
  void validateTopics(Flow flow, DAG<Operator<?, ?>> dag) {
    // nop
  }



}
