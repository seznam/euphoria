/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
package cz.seznam.euphoria.spark;

import com.google.common.annotations.VisibleForTesting;
import cz.seznam.euphoria.core.client.util.Either;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.spark.api.java.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Given an iterator over a data-set of items sorted as follows
 *
 * <p>K1, V1 [LEFT] K1, V2 [LEFT] K1, W1 [RIGHT] K1, W2 [RIGHT] K1, W3 [RIGHT] ... K2, V1 [LEFT] K2,
 * W1 [RIGHT] K2, W2 [RIGHT] ...
 *
 * <p>i.e. items with the same key grouped together and within each group, the (left-side items
 * precedes all right-side items.
 *
 * <p>See also {@link BatchJoinKey}),
 *
 * <p>Left side needs to be cached in memory in order to be able to create a cartesian product.
 *
 * <p>The resulting data-set is in the form of
 *
 * <p>K1, <V1, W1> K1, <V2, W1> K1, <V1, W2> K1, <V2, W2> K1, <V1, W3> K1, <V2, W3> ... K2, <V1, W1>
 * K2, <V1, W1> K2, <V1, W2> ...
 */
class BatchJoinIterator<K, L, R> implements Iterator<Tuple2<K, Tuple2<Optional<L>, Optional<R>>>> {

  private static final Logger LOG = LoggerFactory.getLogger(BatchJoinIterator.class);
  private static final int NUMBER_OF_TOP_KEYS_TO_KEEP = 10;

  /** Decorated iterator */
  private final Iterator<Tuple2<BatchJoinKey<K>, Either<L, R>>> inner;

  /** Cache left sides with tke same key (for cartesian product) */
  private Queue<Tuple2<BatchJoinKey<K>, Either<L, R>>> leftQueue = new LinkedList<>();

  /** Queue the user will iterate on */
  private final Queue<Tuple2<K, Tuple2<Optional<L>, Optional<R>>>> outQueue =
      new LinkedList<>();

  /** Manually bounded container for keys with most values encountered yet. */
  @VisibleForTesting
  final PriorityQueue<StatsItem<K>> topKeys = new PriorityQueue<>();
  /** Number of elements coming from {@link #inner} seen by this iterator so far. */
  private long numOfEncounteredElements = 0;
  /** Number of elements emitted by this {@link Iterator}*/
  private long numOfEmittedElements = 0;

  /** Number of distinct keys coming from {@link #inner}.*/
  private long numberOfKeys = 0;

  private StatsItem<K> currentKeyStats = null;

  private boolean leftSideEmitted = false;

  BatchJoinIterator(Iterator<Tuple2<BatchJoinKey<K>, Either<L, R>>> inner) {
    this.inner = inner;
  }

  @Override
  public boolean hasNext() {
    if (!outQueue.isEmpty()) {
      return true;
    }
    while (inner.hasNext()) {
      final Tuple2<BatchJoinKey<K>, Either<L, R>> tuple = inner.next();
      addElementToStats(tuple);
      final BatchJoinKey<K> sjk = tuple._1;
      switch (sjk.getSide()) {
        case LEFT:
          {
            // ~ new left side, previous one was already joined with right side
            if (leftSideEmitted) {
              leftQueue.clear();
            }
            // ~ there is no right side for this key
            if (!leftQueue.isEmpty() && !sameKey(leftQueue.peek(), tuple)) {
              emitLeft();
            }
            leftQueue.add(tuple);
            leftSideEmitted = false;
            break;
          }
        case RIGHT:
          {
            if (!leftQueue.isEmpty() && sameKey(leftQueue.peek(), tuple)) {
              emitCartesianProduct(tuple);
              leftSideEmitted = true;
            } else {
              // ~ there is no left side for this key
              outQueue.add(
                  new Tuple2<>(
                      sjk.getKey(), new Tuple2<>(Optional.empty(), Optional.of(tuple._2.right()))));
              // ~ and there may be not emitted left side
              if (!leftSideEmitted && !leftQueue.isEmpty()) {
                emitLeft();
              }
            }
            break;
          }
        default:
          throw new IllegalArgumentException(
              "Unexpected BatchJoinKey.Side [" + sjk.getSide() + "] for key [" + sjk + "].");
      }
      if (!outQueue.isEmpty()) {
        return true;
      }
    }
    // ~ they may be loner left side in the end
    if (!leftSideEmitted && !leftQueue.isEmpty()) {
      emitLeft();
      return true;
    }

    if (currentKeyStats != null){
      keepOrScrapKeyStats(currentKeyStats);
    }
    logStats();
    return false;
  }

  @Override
  public Tuple2<K, Tuple2<Optional<L>, Optional<R>>> next() {
    numOfEmittedElements++;
    return outQueue.poll();
  }

  private void emitCartesianProduct(Tuple2<BatchJoinKey<K>, Either<L, R>> right) {
    leftQueue.forEach(
        left ->
            outQueue.add(
                new Tuple2<>(
                    left._1.getKey(),
                    new Tuple2<>(Optional.of(left._2.left()), Optional.of(right._2.right())))));
  }

  private void emitLeft() {
    leftQueue.forEach(
        left ->
            outQueue.add(
                new Tuple2<>(
                    left._1.getKey(),
                    new Tuple2<>(Optional.of(left._2.left()), Optional.empty()))));
    leftQueue.clear();
  }

  private boolean sameKey(Tuple2<BatchJoinKey<K>, ?> a, Tuple2<BatchJoinKey<K>, ?> b) {
    return Objects.equals(a._1.getKey(), b._1.getKey());
  }

  private void addElementToStats(Tuple2<BatchJoinKey<K>, Either<L, R>> element) {

    K key = element._1.getKey();
    if (currentKeyStats == null) {
      currentKeyStats = new StatsItem<>(key);
      numberOfKeys++;
    }

    if (!key.equals(currentKeyStats.key)) { // key has changed, commit statistics
      keepOrScrapKeyStats(currentKeyStats);
      currentKeyStats = new StatsItem<>(key);
      numberOfKeys++;
    }

    Either<L, R> eitherSide = element._2;

    if (eitherSide.isLeft()) {
      currentKeyStats.leftSideElements++;
    } else {
      currentKeyStats.rightSideElements++;
    }

    numOfEncounteredElements++;
  }

  private void keepOrScrapKeyStats(StatsItem<K> finishedStats){
    if (topKeys.size() < NUMBER_OF_TOP_KEYS_TO_KEEP){
      topKeys.add(finishedStats);
      return;
    }

    StatsItem<K> smallestKey = topKeys.peek();
    if(finishedStats.elementsCount()> smallestKey.elementsCount()){
      topKeys.poll();
      topKeys.add(finishedStats);
    }

  }

  private void logStats() {

    if (!LOG.isInfoEnabled()) {
      return;
    }

    LOG.info("-- {} statistics:", BatchJoinIterator.class.getSimpleName());
    LOG.info("-- Keys count: {}, input elements count: {}, output (emitted/joined) elements #: {}.",
        numberOfKeys, numOfEncounteredElements, numOfEmittedElements);

    PriorityQueue<StatsItem<K>> keysToLog = new PriorityQueue<>(topKeys);
    Deque<StatsItem<K>> biggestKeyFirst = new ArrayDeque<>(keysToLog.size());
    StatsItem<K> topItem;
    while ((topItem = keysToLog.poll()) != null) {
      biggestKeyFirst.addFirst(topItem);
    }

    LOG.info("-- top {} keys:", biggestKeyFirst.size());
    biggestKeyFirst.forEach(item -> {
      long l = item.leftSideElements;
      long r = item.rightSideElements;
      K k = item.key;
      LOG.info("---- key: '{}' (hash: {}), input elements count: {} ({} left + {} right)",
          k, k.hashCode(), l + r, l, r);
    });

  }

  @VisibleForTesting
  static class StatsItem<K> implements Comparable<StatsItem<K>>{
    final K key;
    long leftSideElements = 0;
    long rightSideElements = 0;

    public StatsItem(K key) {
      this.key = key;
    }

    @Override
    public int compareTo(StatsItem<K> o) {
      return Long.compare(elementsCount(), o.elementsCount());
    }

    long elementsCount(){
      return leftSideElements + rightSideElements;
    }

  }
}
