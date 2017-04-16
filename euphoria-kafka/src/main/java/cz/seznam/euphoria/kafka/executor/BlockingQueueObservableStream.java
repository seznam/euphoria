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

import cz.seznam.euphoria.inmem.operator.StreamElement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

/**
 * An {@code ObservableStream} backed up by {@code BlockingQueue}.
 */
public class BlockingQueueObservableStream<T extends StreamElement<?>>
    implements ObservableStream<T> {

  /**
   * Create observable stream from given {@code BlockingQueue}.
   * This observable will be able to observe only single partition
   * @param executor the executor to run asynchronous operations with
   * @param operator name of the operator outputting this stream (for debug purposes)
   * @param queue the {@code BlockingQueue} representing single partition of a partitioned stream
   * @param partitionId ID of the partition that the queue represents
   */
  public static <T extends StreamElement<?>> BlockingQueueObservableStream<T> wrap(
      Executor executor,
      String operator,
      BlockingQueue<T> queue,
      int partitionId) {

    BlockingQueueObservableStream<T> ret;
    ret = new BlockingQueueObservableStream<>(
        executor, operator, queue, partitionId);
    ret.runThread();
    return ret;
  }

  final Executor executor;
  final String operator;
  final BlockingQueue<T> queue;
  final int partitionId;
  final Set<String> observerNames = new HashSet<>();
  final List<StreamObserver<T>> observers = new ArrayList<>();
  final Thread forwardThread;

  private BlockingQueueObservableStream(
      Executor executor,
      String operator,
      BlockingQueue<T> queue,
      int partitionId) {

    this.executor = executor;
    this.operator = operator;
    this.queue = queue;
    this.partitionId = partitionId;
    forwardThread = new Thread(this::forwardQueue);
    forwardThread.setDaemon(true);
    forwardThread.setName("blockingQueue-forward-of:" + operator);
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

  private void runThread() {
    this.forwardThread.start();
  }

  void forwardQueue() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        T elem = queue.take();
        if (!elem.isEndOfStream()) {
          synchronized (observers) {
            if (observers.isEmpty()) {
              throw new RuntimeException(
                  "No observers registered for element " + elem + " in queue of operator " + operator);
            }
            observers.forEach(o -> o.onNext(partitionId, elem));
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
        return;
      }
    }
    synchronized (observers) {
      observers.forEach(o -> o.onCompleted());
    }
  }

}
