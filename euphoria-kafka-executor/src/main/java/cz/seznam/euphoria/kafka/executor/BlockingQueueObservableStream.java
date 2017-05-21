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

import cz.seznam.euphoria.inmem.operator.StreamElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@code ObservableStream} backed up by {@code BlockingQueue}.
 */
public class BlockingQueueObservableStream<T extends StreamElement<?>>
    implements ObservableStream<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BlockingQueueObservableStream.class);

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
      List<BlockingQueue<T>> queues) {

    BlockingQueueObservableStream<T> ret;
    ret = new BlockingQueueObservableStream<>(
        executor, operator, queues);
    return ret;
  }

  final Executor executor;
  final String operator;
  final List<BlockingQueue<T>> queues;
  final int numOutputQueues;
  
  final List<StreamObserver<T>> observers = new ArrayList<>();

  final AtomicInteger finished = new AtomicInteger(0);
  final AtomicBoolean error = new AtomicBoolean(false);
  final Exception exc = new RuntimeException();

  private BlockingQueueObservableStream(
      Executor executor,
      String operator,
      List<BlockingQueue<T>> queues) {

    this.executor = executor;
    this.operator = operator;
    this.queues = Objects.requireNonNull(queues);
    this.numOutputQueues = queues.size();
    if (numOutputQueues == 0) {
      throw new IllegalArgumentException(
          "List of outbound queues cannot be zero-length.");
    }
    int partitionId = 0;
    for (BlockingQueue<T> q : queues) {
      int c = partitionId++;
      executor.execute(() -> {
        try {
          forwardQueue(q, c);
        } catch (Exception ex) {
          LOG.error("Error observing stream of operator {}", operator, ex);
        }
      });
    }
  }

  @Override
  public int size() {
    return queues.size();
  }


  @Override
  public void observe(StreamObserver<T> observer) {
    executor.execute(() -> {
      synchronized (observers) {
        observers.add(observer);
        observer.onAssign(numOutputQueues);
      }      
    });
  }

  void forwardQueue(BlockingQueue<T> queue, int partitionId) {
    while (!error.get() && !Thread.currentThread().isInterrupted()) {
      try {
        T elem = queue.take();
        if (!elem.isEndOfStream()) {
          synchronized (observers) {
            if (observers.isEmpty()) {
              LOG.error("No observer for element {}. Trace of constructor of "
                  + "this queue follows:", elem, exc);
              throw new IllegalStateException("Cannot forward element " + elem
                  + " to any observer! This object was created");
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
    if (finished.incrementAndGet() == numOutputQueues) {
      synchronized (observers) {
        observers.forEach(o -> o.onCompleted());
      }
    }
  }

}
