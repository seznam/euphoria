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

import com.google.common.collect.Iterables;
import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.executor.graph.Node;
import cz.seznam.euphoria.core.util.Settings;
import org.apache.tez.dag.api.Vertex;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

class TezExecutorContext {

  private final DAG<Operator<?, ?>> dag;
  private final AccumulatorProvider.Factory accumulatorProvider;
  private final Settings settings;

  private final Map<Operator<?, ?>, Chain<?>> chains = new IdentityHashMap<>();
  private final List<Vertex> vertices = new ArrayList<>();

  TezExecutorContext(
      DAG<Operator<?, ?>> dag,
      AccumulatorProvider.Factory accumulatorProvider,
      Settings settings) {
    this.dag = dag;
    this.accumulatorProvider = accumulatorProvider;
    this.settings = settings;
  }

  List<Chain<?>> getInputs(Operator<?, ?> operator) {
    final List<Node<Operator<?, ?>>> parents = dag.getNode(operator).getParents();
    final List<Chain<?>> inputs = new ArrayList<>(parents.size());
    for (Node<Operator<?, ?>> p : parents) {
      final Chain<?> pout = chains.get(dag.getNode(p.get()).get());
      if (pout == null) {
        throw new IllegalArgumentException(
            "Output chain missing for operator " + p.get().getName());
      }
      inputs.add(pout);
    }
    return inputs;
  }

  Chain<?> getSingleInput(Operator<?, ?> operator) {
    return Iterables.getOnlyElement(getInputs(operator));
  }

  void setOutput(Operator<?, ?> operator, Chain<?> chain) {
    chains.put(operator, chain);
  }

  Chain<?> getOutput(Operator<?, ?> operator) {
    final Chain<?> chain = chains.get(operator);
    if (chain == null) {
      throw new IllegalArgumentException(
          "No output exists for operator " + operator.getName());
    }
    return chain;
  }

  AccumulatorProvider.Factory getAccumulatorProvider() {
    return accumulatorProvider;
  }

  Settings getSettings() {
    return settings;
  }

  List<Vertex> getVertices() {
    return vertices;
  }
}
