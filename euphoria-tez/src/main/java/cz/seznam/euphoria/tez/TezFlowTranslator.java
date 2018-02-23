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
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.executor.graph.Node;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.output.DataSinkOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.output.MROutput;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class converts Euphoria's {@code Flow} into Beam's Pipeline.
 */
class TezFlowTranslator {

  private static final Map<Class, OperatorTranslator> translators = new IdentityHashMap<>();

  static {
    translators.put(FlowUnfolder.InputOperator.class, new InputTranslator());
    translators.put(FlatMap.class, new FlatMapTranslator());
//    translators.put(Union.class, new UnionTranslator());
//    translators.put(WrappedPCollectionOperator.class, WrappedPCollectionOperator::translate);

    // extended operators
    translators.put(ReduceByKey.class, new ReduceByKeyTranslator());
//    translators.put(ReduceStateByKey.class, new ReduceStateByKeyTranslator());
  }

  private final AccumulatorProvider.Factory accumulatorProvider;
  private final Settings settings;

  TezFlowTranslator(AccumulatorProvider.Factory accumulatorProvider, Settings settings) {
    this.accumulatorProvider = accumulatorProvider;
    this.settings = settings;
  }

  List<Vertex> translate(Flow flow) {

    final DAG<Operator<?, ?>> dag = toDag(flow);
    final TezExecutorContext context = new TezExecutorContext(dag, accumulatorProvider, settings);

    dag.traverse()
        .map(Node::get)
        .forEach((operator -> translateOperator(operator, context)));

    return dag.getLeafs()
        .stream()
        .map(Node::get)
        .map(op -> {
          if (op.output().getOutputSink() == null) {
            throw new IllegalStateException("Leaf node must have output sink assigned.");
          }

          final Chain<?> chain = context.getOutput(op);


          final ProcessorDescriptor processorDescriptor =
              ProcessorDescriptor.create(ChainProcessor.class.getName())
                  .setUserPayload(ChainProcessor.toUserPayload(chain));

          final Configuration conf;
          try {
            conf = DataSinkOutputFormat.configure(new Configuration(), op.output().getOutputSink());
          } catch (IOException e) {
            throw new IllegalStateException("Unable to configure data sink.", e);
          }
//          conf.set(JobContext.OUTPUT_FORMAT_CLASS_ATTR, DataSinkOutputFormat.class.getName());

          return Vertex.create("TBD", processorDescriptor)
              .addDataSource("TBD_SOURCE", chain.getSourceDescriptor())
              .addDataSink(
                  "TBD_OUTPUT",
                  MROutput.createConfigBuilder(conf, DataSinkOutputFormat.class)
                      .setDoCommit(true)
                      .build());
        })
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private void translateOperator(Operator<?, ?> op, TezExecutorContext context) {
    if (!translators.containsKey(op.getClass())) {
      throw new UnsupportedOperationException(
          "Operator " + op.getClass().getSimpleName() + " not supported");
    }
    final Chain<?> chain = translators.get(op.getClass()).translate(op, context);
    context.setOutput(op, chain);
  }

  @SuppressWarnings("unchecked")
  private DAG<Operator<?, ?>> toDag(Flow flow) {
    return FlowUnfolder.unfold(flow, operator ->
        translators.containsKey(operator.getClass())
            && translators.get(operator.getClass()).wantTranslate(operator));
  }
}
