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
package cz.seznam.euphoria.core.client.util;

import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.functional.ReduceFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;

import java.io.Serializable;
import java.util.stream.Stream;

/**
 * Provides functions with included config. Config must be {@link Serializable}
 * Useful when its needed pass config with UnaryFunction, BinaryFunctor, UnaryFunctor, ReduceFunctor
 *
 * @param <CONFIG> to include to functions
 */
public class ConfigAware<CONFIG extends ConfigAware.Config> implements Serializable {

  private final CONFIG config;

  public ConfigAware(CONFIG config) {
    this.config = config;
  }

  public <IN, OUT> UnaryFunction<IN, OUT> withConfig(ConfigAwareUnaryFunction<IN, OUT, CONFIG> f) {
    return (in) -> f.apply(in, config);
  }

  public <LEFT, RIGHT, OUT> BinaryFunctor<LEFT, RIGHT, OUT> withConfig(
      ConfigAwareBinaryFunctor<LEFT, RIGHT, OUT, CONFIG> f) {
    return (left, right, coll) -> f.apply(left, right, coll, config);
  }

  public <IN, OUT> UnaryFunctor<IN, OUT> withConfig(ConfigAwareUnaryFunctor<IN, OUT, CONFIG> f) {
    return (in, coll) -> f.apply(in, coll, config);
  }

  public <IN, OUT> ReduceFunctor<IN, OUT> withConfig(ConfigAwareReduceFunctor<IN, OUT, CONFIG> f) {
    return (in, coll) -> f.apply(in, coll, config);
  }

  public interface Config extends Serializable {

  }

  @FunctionalInterface
  public interface ConfigAwareUnaryFunction<IN, OUT, CONFIG extends Config> extends Serializable {
    OUT apply(IN item, CONFIG config);
  }

  @FunctionalInterface
  public interface ConfigAwareUnaryFunctor<IN, OUT, CONFIG extends Config> extends Serializable {
    void apply(IN item, Collector<OUT> c, CONFIG config);
  }

  @FunctionalInterface
  public interface ConfigAwareBinaryFunctor<LEFT, RIGHT, OUT, CONFIG extends Config>
      extends Serializable {
    void apply(LEFT left, RIGHT right, Collector<OUT> ctx, CONFIG config);
  }

  @FunctionalInterface
  public interface ConfigAwareReduceFunctor<IN, OUT, CONFIG extends Config>
      extends Serializable {
    void apply(Stream<IN> left, Collector<OUT> ctx, CONFIG config);
  }
}
