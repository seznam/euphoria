/*
 * Copyright 2016-2019 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.ClassUtils;
import cz.seznam.euphoria.shadow.com.google.common.base.Preconditions;
import cz.seznam.euphoria.shadow.com.google.common.collect.Iterators;
import cz.seznam.euphoria.shadow.com.google.common.collect.Ordering;
import java.io.Serializable;
import java.util.Comparator;
import javax.annotation.Nullable;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;

/**
 * JOIN optimized for batch processing of very large data-sets.
 *
 * <p>----------------------------------------------------------------------------
 *
 * <p>MEMORY EFFICIENCY
 *
 * <p>A simple approach based on standard Spark join operations is very memory-inefficient. This
 * algorithm, on the other hand, only requires to keep in memory only the left-side of currently
 * processed key and at most one right-side item at any time.
 *
 * <p>----------------------------------------------------------------------------
 *
 * <p>SKEWED DATA-SETS
 *
 * <p>where there are large numbers of right-side items that correspond to the same left-side item.
 *
 * <p>----------------------------------------------------------------------------
 *
 * <p>BALANCING PARTITIONS
 *
 * <p>A simple approach based on standard Spark join operations also causes every right-side item
 * corresponding to a single left-side item to end up in the same partition. This is an issue for
 * cases where there are largely different counts of right-side items corresponding to individual
 * left-side items, because it results into large differences in resulting partition sizes.
 *
 * <p>This version will copy each left-side item to {@link #NUM_DUPLICITIES} partitions and then
 * distribute right-side items accordingly. This allows to break large groups of items corresponding
 * to a single left-side item into chunks across partitions.
 *
 * <p>The {@link #NUM_DUPLICITIES} coefficient needs to be chosen carefully, because when too large,
 * the overhead of copying each left-side item multiple times can overweight the benefits.
 */
class BatchJoinTranslator implements SparkOperatorTranslator<Join> {

  @SuppressWarnings("unchecked")
  static boolean wantTranslate(Join join, SparkFlowTranslator.AcceptorContext context) {
    return (join.getWindowing() == null || join.getWindowing() instanceof GlobalWindowing) &&
        (ClassUtils.isComparable(join.getKeyClass()) || context.hasComparator(join.getKeyClass()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(Join operator, SparkExecutorContext context) {

    Preconditions.checkArgument(
        operator.getWindowing() == null || operator.getWindowing() instanceof GlobalWindowing,
        "BatchJoinTranslator only supports GlobalWindowing.");

    final List<JavaRDD<?>> inputs = context.getInputs(operator);
    final JavaRDD<SparkElement> left = (JavaRDD<SparkElement>) inputs.get(0);
    final JavaRDD<SparkElement> right = (JavaRDD<SparkElement>) inputs.get(1);

    final int numPartitions = right.getNumPartitions();

    final JavaPairRDD<BatchJoinKey<Object>, Either<SparkElement, SparkElement>> leftPair =
        left.mapToPair(
                se -> {
                  final Object key = operator.getLeftKeyExtractor().apply(se.getElement());
                  return new Tuple2<>(
                      new BatchJoinKey<>(key, BatchJoinKey.Side.LEFT),
                      Either.<SparkElement, SparkElement>left(se));
                })
            .setName(operator.getName() + "::wrap-keys");

    final JavaPairRDD<BatchJoinKey<Object>, Either<SparkElement, SparkElement>> rightPair =
        right.mapToPair(
            se -> {
              final Object key = operator.getRightKeyExtractor().apply(se.getElement());
              return new Tuple2<>(
                  new BatchJoinKey<>(key, BatchJoinKey.Side.RIGHT),
                  Either.right(se));
            });

    rightPair.setName(operator.getName() + "::wrap-values");

    final Partitioner partitioner = new BatchPartitioner(numPartitions);

    final Comparator<BatchJoinKey<Object>> comparator =
        context.getComparator(operator.getKeyClass()) != null
            ? new BatchJoinKeyComparator<>(context.getComparator(operator.getKeyClass()))
            : new BatchJoinKeyComparator<>(null);

    return leftPair
        .union(rightPair)
        .setName(operator.getName() + "::union-inputs")
        .repartitionAndSortWithinPartitions(partitioner, comparator)
        .setName(operator.getName() + "::sort-by-key-and-side")
        .mapPartitions(
            iterator -> new JoinIterator<>(new BatchJoinIterator<>(iterator), operator.getType()))
        .setName(operator.getName() + "::create-iterator")
        .flatMap(
            new FlatMapFunctionWithCollector<>(
                (t, collector) -> {
                  // ~ both elements have exactly the same window
                  // we need to check for null values because of full outer join
                  final SparkElement first = t._2._1.orNull();
                  final SparkElement second = t._2._2.orNull();
                  final long maxTimestamp =
                      Math.max(
                          first == null ? 0L : first.getTimestamp(),
                          second == null ? 0L : second.getTimestamp());
                  collector.clear();
                  collector.setWindow(GlobalWindowing.Window.get());
                  operator
                      .getJoiner()
                      .apply(
                          first == null ? null : first.getElement(),
                          second == null ? null : second.getElement(),
                          collector);
                  return Iterators.transform(
                      collector.getOutputIterator(),
                      e ->
                          new SparkElement<>(
                              GlobalWindowing.Window.get(), maxTimestamp, Pair.of(t._1, e)));
                },
                new LazyAccumulatorProvider(
                    context.getAccumulatorFactory(), context.getSettings())))
        .setName(operator.getName() + "::apply-udf-and-wrap-in-spark-element");
  }

  /**
   * Maps the set of all integer numbers to the set of positive integer numbers, with minimum
   * collisions. Returns the positive integer corresponding to the given value.
   *
   * <p>This is needed when indexing into an array based on an objects {@link Object#hashCode()}
   * modulo size of the array.
   *
   * <p>It is necessary because:
   *
   * <ul>
   *   <li>{@link Object#hashCode()} can be a negative number and
   *   <li>{@link Math#abs} returns a negative number for {@link Integer#MIN_VALUE} and therefore
   *       cannot be used instead.
   * </ul>
   */
  private static int positive(int value) {
    return value & Integer.MAX_VALUE;
  }

  private static class BatchPartitioner extends Partitioner {

    private final int numPartitions;

    BatchPartitioner(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }

    @Override
    public int getPartition(Object obj) {
      int x = positive(((BatchJoinKey) obj).getKey().hashCode()) % numPartitions;
      return positive(((BatchJoinKey) obj).getKey().hashCode()) % numPartitions;
    }
  }

  private static class BatchJoinKeyComparator<K>
      implements Comparator<BatchJoinKey<K>>, Serializable {

    @Nullable
    private final Comparator<K> delegate;

    BatchJoinKeyComparator(@Nullable Comparator<K> delegate) {
      this.delegate = delegate;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(BatchJoinKey<K> o1, BatchJoinKey<K> o2) {
      final int keyResult;
      if (delegate == null) {
        // we know that key is comparable
        keyResult = ((Comparable<K>) o1.getKey()).compareTo(o2.getKey());
      } else {
        keyResult = delegate.compare(o1.getKey(), o2.getKey());
      }
      if (keyResult == 0) {
        return compareSide(o1.getSide(), o2.getSide());
      }
      return keyResult;
    }

    private static int compareSide(BatchJoinKey.Side a, BatchJoinKey.Side b) {
      // ~ LEFT side precedes RIGHT side
      if (a.equals(b)) {
        return 0;
      }
      if (a.equals(BatchJoinKey.Side.LEFT)) {
        return -1;
      }
      return 1;
    }
  }
}
