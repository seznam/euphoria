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

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.Vertex;

import java.util.stream.StreamSupport;

public class ReduceByKeyTranslator implements OperatorTranslator<ReduceByKey> {

  @Override
  public Chain translate(ReduceByKey operator, TezExecutorContext context) {
    return null;
  }

  @SuppressWarnings("unchecked")
  private static <IN, KEY, VALUE, OUT, W extends Window<W>> Chain<Pair<KEY, OUT>> doTranslate(
      ReduceByKey<IN, KEY, VALUE, OUT, W> operator, TezExecutorContext context) {

    final Chain<IN> inputChain = (Chain<IN>) context.getSingleInput(operator);
    final TezAccumulatorProvider accumulatorProvider = TezAccumulatorProvider.of(context);

    if (operator.getWindowing() != null) {
      final Windowing<IN, W> windowing = operator.getWindowing();
      inputChain.modify((stream) ->
          stream.flatMap((element) -> {
            final Iterable<W> windows =
                windowing.assignWindowsToElement((TezElement<W, IN>) element);
            return StreamSupport.stream(windows.spliterator(), false)
                .map(window -> window);
          }));
    }

    final ProcessorDescriptor processorDescriptor =
        ProcessorDescriptor.create(ChainProcessor.class.getName())
            .setUserPayload(ChainProcessor.toUserPayload(inputChain));


    final Vertex vertex =
        Vertex.create("reduce-by-key::" + operator.getName(), processorDescriptor);

    context.getVertices().add(vertex);

//    return inputChain.modify((stream) ->
//        stream.flatMap(new StreamFunction<>(operator, accumulatorProvider)));
    return null;
  }

  private static class KeyedWindow {

  }

}
