/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.operator.SideOutput;

/**
 * Extends {@link Environment} with write capability. Used in
 * user defined functors.
 *
 * @param <T> the type of elements collected through this context
 */
@Audience(Audience.Type.CLIENT)
public interface Collector<T> extends Environment {

  /**
   * Collects the given element to the output of this context.
   *
   * @param elem the element to collect
   */
  void collect(T elem);

  /**
   * Collects the given element to the side output of this context.
   *
   * @param sideOutput output to collect element to
   * @param elem the element to collect
   */
  default <OUT> void collect(SideOutput<OUT> sideOutput, OUT elem) {
    throw new UnsupportedOperationException("Side outputs are not supported with this collector.");
  }


  /**
   * Returns {@link Context} view of the collector.
   * Since {@link Collector} usually share the same methods
   * as {@link Context} it can be safely casted.
   *
   * @return this instance as a context class
   */
  Context asContext();
}