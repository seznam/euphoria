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

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.util.SingleValueContext;
import cz.seznam.euphoria.core.util.ClassUtils;
import cz.seznam.euphoria.shadow.com.google.common.base.Preconditions;
import cz.seznam.euphoria.shadow.com.google.common.collect.Iterators;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class ReduceByKeyTranslator implements SparkOperatorTranslator<ReduceByKey> {

  private static final Logger LOG = LoggerFactory.getLogger(ReduceByKeyTranslator.class);

  static boolean wantTranslate(ReduceByKey operator, SparkFlowTranslator.AcceptorContext context) {
    return (operator.getValueComparator() == null
            || ClassUtils.isComparable(operator.getKeyClass()))
        && (operator.getWindowing() == null
            || (!(operator.getWindowing() instanceof MergingWindowing)
                && !operator.getWindowing().getTrigger().isStateful()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(ReduceByKey operator, SparkExecutorContext context) {
    return doTranslate(operator, context);
  }

  private <IN, KEY, VALUE, OUT, W extends Window>
      JavaRDD<SparkElement<W, Pair<KEY, OUT>>> doTranslate(
          ReduceByKey<IN, KEY, VALUE, OUT, W> operator, SparkExecutorContext context) {

    final JavaRDD<SparkElement<?, IN>> input = context.getSingleInput(operator);
    final ReduceFunctor<VALUE, OUT> reducer = operator.getReducer();

    @SuppressWarnings("unchecked")
    final Windowing<IN, W> windowing =
        operator.getWindowing() == null ? AttachedWindowing.INSTANCE : operator.getWindowing();

    final UnaryFunction<IN, KEY> keyExtractor = operator.getKeyExtractor();
    final UnaryFunction<IN, VALUE> valueExtractor = operator.getValueExtractor();

    Preconditions.checkState(
        !(windowing instanceof MergingWindowing), "MergingWindowing not supported!");
    Preconditions.checkState(
        !windowing.getTrigger().isStateful(), "Stateful triggers not supported!");

    // ~ extract key/value + timestamp from input elements and assign windows
    final JavaPairRDD<KeyedWindow<W, KEY>, VALUE> tuples =
        input
            .flatMapToPair(new CompositeKeyExtractor<>(keyExtractor, valueExtractor, windowing))
            .setName(operator.getName() + "::extract-key-values");

    final AccumulatorProvider accumulatorProvider =
        new LazyAccumulatorProvider(context.getAccumulatorFactory(), context.getSettings());

    // ~ this code can not be reused due to different reducer signature
    if (operator.isCombinable()) {
      @SuppressWarnings("unchecked")
      final ReduceFunctor<VALUE, VALUE> combiner = (ReduceFunctor<VALUE, VALUE>) reducer;
      final JavaPairRDD<KeyedWindow<W, KEY>, VALUE> combined =
          tuples
              .reduceByKey(new CombinableReducer<>(combiner))
              .setName(operator.getName() + "::combine-by-key");

      return combined
          .map(
              t -> {
                final KeyedWindow<W, KEY> kw = t._1();
                @SuppressWarnings("unchecked")
                final OUT el = (OUT) t._2();
                return new SparkElement<>(kw.window(), kw.timestamp(), Pair.of(kw.key(), el));
              })
          .setName(operator.getName() + "::wrap-in-spark-element");
    }

    final JavaPairRDD<KeyedWindow<W, KEY>, OUT> reduced;

    if (ClassUtils.isComparable(operator.getKeyClass())) {
      final Partitioner partitioner = new HashPartitioner(input.getNumPartitions());

      if (operator.getValueComparator() != null) {
        // if we have a value comparator we need to secondary sort the values within key
        reduced =
            tuples
                .mapToPair(t -> new Tuple2<>(new KeyedWindowValue<>(t._1, t._2), Empty.get()))
                .setName(operator.getName() + "::create-composite-key")
                .repartitionAndSortWithinPartitions(
                    partitioner, new SecondarySortComparator<>(operator.getValueComparator()))
                .setName(operator.getName() + "::secondary-sort")
                .mapToPair(t -> new Tuple2<>(t._1.toKeyedWindow(), t._1.getValue()))
                .setName(operator.getName() + "::unwrap-composite-key")
                .mapPartitionsToPair(ReduceByKeyIterator::new)
                .setName(operator.getName() + "::create-iterator")
                .flatMapValues(new Reducer<>(reducer, accumulatorProvider))
                .setName(operator.getName() + "::apply-udf");
      } else {
        // if the key is comparable, we can optimize for memory efficiency
        reduced =
            tuples
                .repartitionAndSortWithinPartitions(partitioner)
                .setName(operator.getName() + "::sort")
                .mapPartitionsToPair(ReduceByKeyIterator::new)
                .setName(operator.getName() + "::create-iterator")
                .flatMapValues(new Reducer<>(reducer, accumulatorProvider))
                .setName(operator.getName() + "::apply-udf");
      }
    } else {
      LOG.warn(
          "Key for ["
              + operator.getName()
              + "] is not comparable, so we can not optimize for memory efficiency.");
      reduced =
          tuples
              .groupByKey()
              .setName(operator.getName() + "::group-by-key")
              .flatMapValues(new Reducer<>(reducer, accumulatorProvider))
              .setName(operator.getName() + "::apply-udf");
    }

    return reduced
        .map(
            t -> {
              final KeyedWindow<W, KEY> kw = t._1();
              final OUT el = t._2();
              return new SparkElement<>(kw.window(), kw.timestamp(), Pair.of(kw.key(), el));
            })
        .setName(operator.getName() + "::wrap-in-spark-element");
  }

  /**
   * Extracts {@link KeyedWindow} from {@link SparkElement} and assigns timestamp according to
   * (optional) eventTimeAssigner.
   */
  private static class CompositeKeyExtractor<IN, KEY, VALUE, W extends Window>
      implements PairFlatMapFunction<SparkElement<?, IN>, KeyedWindow<W, KEY>, VALUE> {

    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final Windowing<IN, W> windowing;

    CompositeKeyExtractor(
        UnaryFunction<IN, KEY> keyExtractor,
        UnaryFunction<IN, VALUE> valueExtractor,
        Windowing<IN, W> windowing) {
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
      this.windowing = windowing;
    }

    @Override
    public Iterator<Tuple2<KeyedWindow<W, KEY>, VALUE>> call(SparkElement<?, IN> wel) {
      final Iterable<W> windows = windowing.assignWindowsToElement(wel);
      return Iterators.transform(
          windows.iterator(),
          wid -> {
            final long stamp = Objects.requireNonNull(wid).maxTimestamp() - 1;
            return new Tuple2<>(
                new KeyedWindow<>(wid, stamp, keyExtractor.apply(wel.getElement())),
                valueExtractor.apply(wel.getElement()));
          });
    }
  }

  private static class Reducer<IN, OUT> implements Function<Iterable<IN>, Iterable<OUT>> {

    private final ReduceFunctor<IN, OUT> reducer;
    private final AccumulatorProvider accumulatorProvider;

    private transient FunctionCollectorMem<OUT> collector;

    private Reducer(ReduceFunctor<IN, OUT> reducer, AccumulatorProvider accumulatorProvider) {
      this.reducer = reducer;
      this.accumulatorProvider = accumulatorProvider;
    }

    @Override
    public Iterable<OUT> call(Iterable<IN> input) {
      if (collector == null) {
        collector = new FunctionCollectorMem<>(accumulatorProvider);
      }
      collector.clear();
      reducer.apply(StreamSupport.stream(input.spliterator(), false), collector);
      return () -> collector.getOutputIterator();
    }
  }

  private static class CombinableReducer<IN> implements Function2<IN, IN, IN> {

    private final ReduceFunctor<IN, IN> reducer;
    private transient SingleValueContext<IN> context;

    private CombinableReducer(ReduceFunctor<IN, IN> reducer) {
      this.reducer = reducer;
    }

    @Override
    public IN call(IN o1, IN o2) {
      if (context == null) {
        context = new SingleValueContext<>();
      }
      reducer.apply(Stream.of(o1, o2), context);
      return context.getAndResetValue();
    }
  }

  /**
   * We need pass value into keyed window in order to implement secondary sort.
   *
   * @param <W> window type
   * @param <K> key type
   * @param <V> value type
   */
  static class KeyedWindowValue<W extends Window, K, V> extends KeyedWindow<W, K> {

    private final V value;

    KeyedWindowValue(KeyedWindow<W, K> keyedWindow, V value) {
      super(keyedWindow.window(), keyedWindow.timestamp(), keyedWindow.key());
      this.value = value;
    }

    public V getValue() {
      return value;
    }

    public KeyedWindow<W, K> toKeyedWindow() {
      return new KeyedWindow<>(window(), timestamp(), key());
    }
  }

  /**
   * Sort values within a keyed window.
   *
   * @param <W> window type
   * @param <K> key type
   * @param <V> value type
   */
  private static class SecondarySortComparator<W extends Window, K, V>
      implements Comparator<KeyedWindowValue<W, K, V>>, Serializable {

    private final BinaryFunction<V, V, Integer> valueComparator;

    SecondarySortComparator(BinaryFunction<V, V, Integer> valueComparator) {
      this.valueComparator = Objects.requireNonNull(valueComparator);
    }

    @Override
    public int compare(KeyedWindowValue<W, K, V> o1, KeyedWindowValue<W, K, V> o2) {
      final int keyedWindowCompare = o1.compareTo(o2);
      if (keyedWindowCompare == 0) {
        // we are in the same window and same key - lets do secondary sort
        return valueComparator.apply(o1.getValue(), o2.getValue());
      }
      return keyedWindowCompare;
    }
  }
}
