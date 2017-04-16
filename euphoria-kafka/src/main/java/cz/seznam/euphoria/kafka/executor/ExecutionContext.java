/*
 * Copyright 2016-2017 Seznam.cz, a.s..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.kafka.executor;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A context for providing readers for datasets.
 */
public class ExecutionContext {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);

  final Map<Dataset<?>, ObservableStream<KafkaStreamElement>> streams = new HashMap<>();

  public void register(Dataset<?> dataset, ObservableStream<KafkaStreamElement> stream) {
    streams.put(dataset, stream);
    if (LOG.isDebugEnabled()) {
      if (dataset.getProducer() != null) {
        LOG.debug(
            "Added stream {} for dataset {}, which is output of {}",
            new String[] {
              stream.toString(), dataset.toString(),
              dataset.getProducer().toString()
            });
      } else {
        LOG.debug("Added stream {} for INPUT dataset {} with source {}",
            new String[] {
              stream.toString(), dataset.toString(), dataset.getSource().toString()
            });
      }
    }
  }

  public ObservableStream<KafkaStreamElement> get(Dataset<?> dataset) {
    return Objects.requireNonNull(
        streams.get(dataset),
        "Cannot find stream for dataset " + dataset
            + " produced by " + dataset.getProducer());
  }

}
