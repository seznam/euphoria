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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * An {@code ObservableStream} backed up by {@code BlockingQueue}.
 */
public class BlockingQueueObservableStream<T> implements ObservableStream<T> {

  /**
   * Create observable stream from given {@code BlockingQueue}.
   * This observable will be able to observe only single partition
   * @param queue the {@code BlockingQueue} representing single partition of a partitioned stream
   * @param partitionId ID of the partition that the queue represents
   */
  public static <T> BlockingQueueObservableStream<T> wrap(
      BlockingQueue<T> queue,
      int partitionId) {

    return new BlockingQueueObservableStream<>(queue, partitionId);
  }

  final BlockingQueue<T> queue;
  final int partitionId;
  final Set<String> observerNames = new HashSet<>();
  final List<StreamObserver<T>> observers = new ArrayList<>();
  final Thread forwardThread;

  private BlockingQueueObservableStream(
      BlockingQueue<T> queue,
      int partitionId) {

    this.queue = queue;
    this.partitionId = partitionId;
    forwardThread = new Thread(this::forwardQueue);
  }

  @Override
  public void observe(String name, StreamObserver<T> observer) {
    if (observerNames.contains(name)) {
      // FIXME: support this!!!
      throw new UnsupportedOperationException(
          "In this POC, branching of in-process streams is not supported.");
    }
    synchronized (observers) {
      observers.add(observer);
    }
    observer.onRegistered();
  }

  void forwardQueue() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        T elem = queue.take();
        synchronized (observers) {
          observers.forEach(o -> o.onNext(partitionId, elem));
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
