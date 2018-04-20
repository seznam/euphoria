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

import com.google.common.collect.Iterators;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.hint.SizeHint;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 * <p>
 *   Broadcast hash join is a special case of Left or Right join, which does not require shuffle.
 *   The optional side is loaded into memory and broadcasted to all executors. We can then use
 *   map side join with lookups to in memory hash table on non-optional side.
 * </p>
 * <p>
 *   In order to use this translator, you need to have on one Dataset
 *   {@link SizeHint#FITS_IN_MEMORY} hint
 *   to the {@link Join} operator.
 * </p>
 */
public class JoinTranslator implements SparkOperatorTranslator<Join> {

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(Join operator, SparkExecutorContext context) {

    final List<JavaRDD<?>> inputs = context.getInputs(operator);
    final JavaRDD<SparkElement> left = (JavaRDD<SparkElement>) inputs.get(0);
    final JavaRDD<SparkElement> right = (JavaRDD<SparkElement>) inputs.get(1);

    final Windowing windowing = operator.getWindowing() == null
        ? AttachedWindowing.INSTANCE
        : operator.getWindowing();

    final JavaPairRDD<KeyedWindow, SparkElement> leftPair =
        left.flatMapToPair(new KeyExtractor(operator.getLeftKeyExtractor(), windowing, true));
    final JavaPairRDD<KeyedWindow, SparkElement> rightPair =
        right.flatMapToPair(new KeyExtractor(operator.getRightKeyExtractor(), windowing, false));

    final JavaPairRDD<KeyedWindow, Tuple2<Optional<SparkElement>, Optional<SparkElement>>> joined;

    switch (operator.getType()) {
      case LEFT: {
        joined = leftPair.leftOuterJoin(rightPair).flatMapValues((t) ->
            Collections.singletonList(new Tuple2<>(opt(t._1), t._2)));
        break;
      }
      case RIGHT: {
        joined = leftPair.rightOuterJoin(rightPair).flatMapValues((t) ->
            Collections.singletonList(new Tuple2<>(t._1, opt(t._2))));
        break;
      }
      case FULL:
        joined = leftPair.fullOuterJoin(rightPair).flatMapValues((t) ->
            Collections.singletonList(new Tuple2<>(t._1, t._2)));
        break;
      case INNER:
        joined = leftPair.join(rightPair).flatMapValues((t) ->
            Collections.singletonList(new Tuple2<>(opt(t._1), opt(t._2))));
        break;
      default: {
        throw new IllegalStateException("Invalid type: " + operator.getType() + ".");
      }
    }

    return joined.flatMap(new FlatMapFunctionWithCollector<>((t, collector) -> {
      // ~ both elements have exactly the same window
      // ~ we need to check for null values because we support both Left and Right joins
      final SparkElement first = t._2._1.orNull();
      final SparkElement second = t._2._2.orNull();
      final Window window = first == null ? second.getWindow() : first.getWindow();
      final long maxTimestamp = Math.max(
          first == null ? window.maxTimestamp() - 1 : first.getTimestamp(),
          second == null ? window.maxTimestamp() - 1 : second.getTimestamp());
      collector.clear();
      collector.setWindow(window);
      operator.getJoiner().apply(
          first == null ? null : first.getElement(),
          second == null ? null : second.getElement(),
          collector);
      return Iterators.transform(collector.getOutputIterator(), e ->
          new SparkElement<>(window, maxTimestamp, Pair.of(t._1.key(), e)));
    }, new LazyAccumulatorProvider(context.getAccumulatorFactory(), context.getSettings())));
  }

  private static <T> Optional<T> opt(T val) {
    return Optional.ofNullable(val);
  }

  private static class KeyExtractor
      implements PairFlatMapFunction<SparkElement, KeyedWindow, SparkElement> {

    private final UnaryFunction keyExtractor;
    private final Windowing windowing;
    private final boolean left;

    KeyExtractor(UnaryFunction keyExtractor, Windowing windowing, boolean left) {
      this.keyExtractor = keyExtractor;
      this.windowing = windowing;
      this.left = left;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Tuple2<KeyedWindow, SparkElement>> call(SparkElement se) throws Exception {
      final Iterable<Window> windows = windowing.assignWindowsToElement(new SparkElement(
          se.getWindow(),
          se.getTimestamp(),
          left
              ? Either.left(se.getElement())
              : Either.right(se.getElement())));
      return Iterators.transform(windows.iterator(), w -> new Tuple2<>(
          new KeyedWindow<>(w, se.getTimestamp(), keyExtractor.apply(se.getElement())),
          new SparkElement(w, se.getTimestamp(), se.getElement())));
    }
  }
}
