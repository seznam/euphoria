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

import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.shadow.com.google.common.base.Preconditions;
import cz.seznam.euphoria.shadow.com.google.common.collect.Iterators;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
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
  static boolean wantTranslate(Join join) {
    return (join.getWindowing() == null || join.getWindowing() instanceof GlobalWindowing)
        && (join.getKeyClass() != null && Comparable.class.isAssignableFrom(join.getKeyClass()));
  }

  /**
   * Number of partitions across which to distribute right-side items for individual left-side items
   * (also number of copies of each left-side item to make)
   */
  private static final int NUM_DUPLICITIES = 1;

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
        left.flatMapToPair(
            se -> {
              final Object key = operator.getLeftKeyExtractor().apply(se.getElement());
              return selectPartitionIdxsForKey(key, numPartitions, NUM_DUPLICITIES)
                  .stream()
                  .map(
                      partitionIdx ->
                          new Tuple2<>(
                              new BatchJoinKey<>(key, BatchJoinKey.Side.LEFT, partitionIdx),
                              Either.<SparkElement, SparkElement>left(se)))
                  .iterator();
            });

    final JavaPairRDD<BatchJoinKey<Object>, Either<SparkElement, SparkElement>> rightPair =
        right.mapToPair(
            se -> {
              final Object key = operator.getRightKeyExtractor().apply(se.getElement());
              final List<Integer> partitionIdxs =
                  selectPartitionIdxsForKey(key, numPartitions, NUM_DUPLICITIES);
              final int partitionIdx = selectPartitionIdx(se, partitionIdxs);
              return new Tuple2<>(
                  new BatchJoinKey<>(key, BatchJoinKey.Side.RIGHT, partitionIdx), Either.right(se));
            });

    final Partitioner partitioner = new BatchPartitioner(numPartitions);

    return leftPair
        .union(rightPair)
        .repartitionAndSortWithinPartitions(partitioner)
        .mapPartitions(
            iterator -> new JoinIterator<>(new BatchJoinIterator<>(iterator), operator.getType()))
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
                    context.getAccumulatorFactory(), context.getSettings())));
  }

  private static <KEY> List<Integer> selectPartitionIdxsForKey(
      KEY key, int numPartitions, int numDuplicities) {

    final List<Integer> result = new ArrayList<>(numDuplicities);

    int startIdx = positive(key.hashCode()) % numPartitions;
    result.add(startIdx);

    int offset = numPartitions / numDuplicities;

    int count = 1;
    int nextIdx = startIdx;
    while (count < numDuplicities) {
      nextIdx = (nextIdx + offset) % numPartitions;
      result.add(nextIdx);
      count++;
    }

    return result;
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

  private static int selectPartitionIdx(Object item, List<Integer> list) {
    return list.get(positive(item.hashCode()) % list.size());
  }

  /** Partitioner that resolves partition from {@link BatchJoinKey#getPartitionIdx} */
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
      return ((BatchJoinKey) obj).getPartitionIdx();
    }
  }
}
