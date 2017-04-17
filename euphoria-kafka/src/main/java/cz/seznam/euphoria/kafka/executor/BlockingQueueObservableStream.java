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

import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.inmem.operator.StreamElement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@code ObservableStream} backed up by {@code BlockingQueue}.
 */
public class BlockingQueueObservableStream<T extends StreamElement<?>>
    implements ObservableStream<T> {

  /**
   * Create observable stream from given {@code BlockingQueue}.
   * This observable will be able to observe only single partition
   * @param <T> the executor specific {@code StreamElement}
   * @param executor the executor to run asynchronous operations with
   * @param operator name of the operator outputting this stream (for debug purposes)
   * @param queues pair of {@code BlockingQueue} representing partitions of a partitioned stream
   *               and corresponding partition ID
   * @return the {@code BlockingQueueObservableStream} that wraps the queues
   */
  public static <T extends StreamElement<?>> BlockingQueueObservableStream<T> wrap(
      Executor executor,
      String operator,
      List<Pair<BlockingQueue<T>, Integer>> queues) {

    BlockingQueueObservableStream<T> ret;
    ret = new BlockingQueueObservableStream<>(
        executor, operator, queues);
    return ret;
  }

  final Executor executor;
  final String operator;
  final List<Pair<BlockingQueue<T>, Integer>> queues;
  
  final Set<String> observerNames = new HashSet<>();
  final List<StreamObserver<T>> observers = new ArrayList<>();

  final AtomicInteger finished = new AtomicInteger(0);
  final AtomicBoolean error = new AtomicBoolean(false);

  private BlockingQueueObservableStream(
      Executor executor,
      String operator,
      List<Pair<BlockingQueue<T>, Integer>> queues) {

    this.executor = executor;
    this.operator = operator;
    this.queues = queues;
    for (Pair<BlockingQueue<T>, Integer> q : queues) {
      executor.execute(() -> {
        forwardQueue(q.getFirst(), q.getSecond());
      });
    }
  }

  @Override
  public void observe(String name, StreamObserver<T> observer) {
    if (observerNames.contains(name)) {
      // we do not support rebalancing of partitions at this time
      throw new UnsupportedOperationException(
          "Multiple consumers of in-process streams is not supported.");
    }
    executor.execute(() -> {
      synchronized (observers) {
        observers.add(observer);
      }
      observer.onRegistered();
    });
  }

  void forwardQueue(BlockingQueue<T> queue, int partitionId) {
    while (!error.get() && !Thread.currentThread().isInterrupted()) {
      try {
        T elem = queue.take();
        if (!elem.isEndOfStream()) {
          synchronized (observers) {
            if (observers.isEmpty()) {
              throw new RuntimeException(
                  "No observers registered for element "
                      + elem + " in queue of operator " + operator);
            }
            observers.forEach(o -> {
              synchronized (o) {
                o.onNext(partitionId, elem);
              }
            });
          }
        } else {
          break;
        }
      } catch (InterruptedException ex) {
        break;
      } catch (Throwable thrwbl) {
        synchronized (observers) {
          observers.forEach(o -> o.onError(thrwbl));
        }
        error.set(true);
        return;
      }
    }
    if (finished.incrementAndGet() == queues.size()) {
      synchronized (observers) {
        observers.forEach(o -> o.onCompleted());
      }
    }
  }

  @Override
  public int size() {
    return queues.size();
  }

}
