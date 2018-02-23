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
import cz.seznam.euphoria.core.client.accumulators.VoidAccumulatorProvider;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.executor.AbstractExecutor;
import cz.seznam.euphoria.core.util.ExceptionUtils;
import cz.seznam.euphoria.core.util.Settings;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Executor implementation using Apache Beam as a runtime.
 */
public class TezExecutor extends AbstractExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(TezExecutor.class);

  private AccumulatorProvider.Factory accumulatorProvider =
      VoidAccumulatorProvider.Factory.get();

  protected Result execute(Flow flow) {

    final TezConfiguration conf = new TezConfiguration();
    conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    conf.set("fs.defaultFS", "file:///");
    conf.setBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

    final TezClient tezClient = TezClient.create(getClass().getSimpleName(), conf);

    try {
      tezClient.start();
      tezClient.waitTillReady();
      final DAG dag = DAG.create(flow.getName());

      final TezFlowTranslator flowTranslator =
          new TezFlowTranslator(accumulatorProvider, new Settings());

      // todo rollback
      List<Vertex> vertices = flowTranslator.translate(flow);
      vertices.forEach(dag::addVertex);

      final DAGClient dagClient = tezClient.submitDAG(dag);
      final DAGStatus dagStatus = dagClient.waitForCompletion();
      if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
        LOG.info("DAG diagnostics: " + dagStatus.getDiagnostics());
        throw new RuntimeException("Job has failed.");
      }
    } catch (Exception e) {
      throw new IllegalStateException("Unable to run tez job.", e);
    } finally {
      try {
        tezClient.stop();
      } catch (TezException | IOException e) {
        LOG.warn("Unable to gracefully close tez client.", e);
      }
    }
    return new Result();
  }

  @Override
  public void setAccumulatorProvider(AccumulatorProvider.Factory accumulatorProvider) {
    this.accumulatorProvider = accumulatorProvider;
  }
}
