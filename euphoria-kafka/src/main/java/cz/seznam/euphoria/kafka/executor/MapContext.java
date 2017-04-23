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

import cz.seznam.euphoria.core.client.io.Context;
import java.util.concurrent.BlockingQueue;

/**
 * A {@code Context} used by {@code FlatMap}.
 */
class MapContext<T> implements Context<T> {

  final BlockingQueue<KafkaStreamElement> queue;
  KafkaStreamElement input;

  MapContext(BlockingQueue<KafkaStreamElement> queue) {
    this.queue = queue;
  }

  void setInput(KafkaStreamElement input) {
    this.input = input;
  }

  @Override
  public void collect(T elem) {
    try {
      queue.put(KafkaStreamElement.FACTORY.data(
          elem, input.getWindow(), input.getTimestamp()));
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public Object getWindow() {
    return input.getWindow();
  }

}
