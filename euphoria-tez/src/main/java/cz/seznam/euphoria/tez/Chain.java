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

import org.apache.tez.dag.api.DataSourceDescriptor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Chain of operations that do not require shuffle.
 */
class Chain<IN> implements Serializable {

  @FunctionalInterface
  interface StreamModifier<IN, OUT> extends Serializable {

    Stream<OUT> modifyStream(Stream<IN> stream);
  }

  static <IN> Chain<IN> of(DataSourceDescriptor source) {
    return new Chain<>(source, new ArrayList<>());
  }

  private transient final DataSourceDescriptor source;
  private final List<StreamModifier<?, ?>> modifiers;

  private Chain(DataSourceDescriptor source, List<StreamModifier<?, ?>> modifiers) {
    this.source = source;
    this.modifiers = modifiers;
  }

  <OUT> Chain<OUT> modify(StreamModifier<IN, OUT> modifier) {
    modifiers.add(modifier);
    return new Chain<>(source, modifiers);
  }

  DataSourceDescriptor getSourceDescriptor() {
    return source;
  }

  List<StreamModifier<?, ?>> getModifiers() {
    return modifiers;
  }
}
