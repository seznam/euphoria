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
package cz.seznam.euphoria.tez;

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.functional.ExtractEventTime;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.operator.FlatMap;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

class FlatMapTranslator implements OperatorTranslator<FlatMap> {

  @Override
  @SuppressWarnings("unchecked")
  public Chain<?> translate(FlatMap operator, TezExecutorContext context) {
    return doTranslate(operator, context);
  }

  @SuppressWarnings("unchecked")
  private <IN, OUT> Chain<OUT> doTranslate(
      FlatMap<IN, OUT> operator, TezExecutorContext context) {
    final Chain<IN> inputChain = (Chain<IN>) context.getSingleInput(operator);
    final TezAccumulatorProvider accumulatorProvider = TezAccumulatorProvider.of(context);
    return inputChain.modify((stream) ->
        stream.flatMap(new StreamFunction<>(operator, accumulatorProvider)));
  }

  static class StreamFunction<IN, OUT> implements Function<IN, Stream<OUT>> {

    private final UnaryFunctor<IN, OUT> mapper;
    @Nullable
    private final ExtractEventTime<IN> eventTimeExtractor;
    private final ListCollector<OUT> collector;

    StreamFunction(
        FlatMap<IN, OUT> operator,
        TezAccumulatorProvider accumulatorProvider) {
      this.mapper = operator.getFunctor();
      this.eventTimeExtractor = operator.getEventTimeExtractor();
      this.collector = new ListCollector<>(accumulatorProvider.create());

    }

    @Override
    public Stream<OUT> apply(IN in) {
      if (eventTimeExtractor == null) {
        mapper.apply(in, collector);
      } else {
        throw new UnsupportedOperationException("Not implemented.");
      }
      final List<OUT> collected = new ArrayList<>(collector.get());
      collector.clear();
      return collected.stream();
    }
  }
}
