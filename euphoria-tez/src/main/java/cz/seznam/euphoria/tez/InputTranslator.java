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

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.hadoop.input.DataSourceInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.mapreduce.input.MRInput;

import java.io.IOException;

public class InputTranslator implements OperatorTranslator<FlowUnfolder.InputOperator> {

  @Override
  @SuppressWarnings("unchecked")
  public Chain translate(FlowUnfolder.InputOperator operator, TezExecutorContext context) {
    return doTranslate(operator, context);
  }

  private <IN> Chain doTranslate(
      FlowUnfolder.InputOperator<IN> operator, TezExecutorContext context) {
    final DataSource<IN> source = operator.output().getSource();

    final Configuration conf;
    try {
      conf = DataSourceInputFormat.configure(new Configuration(), source);
      // TODO parameter tuning
      final DataSourceDescriptor dataSourceDescriptor =
          MRInput.createConfigBuilder(conf, DataSourceInputFormat.class)
              .build();
      return Chain.of(dataSourceDescriptor);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to configure data source.", e);
    }
  }
}
