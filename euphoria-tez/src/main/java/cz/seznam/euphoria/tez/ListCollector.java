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
import cz.seznam.euphoria.core.client.accumulators.Counter;
import cz.seznam.euphoria.core.client.accumulators.Histogram;
import cz.seznam.euphoria.core.client.accumulators.Timer;
import cz.seznam.euphoria.core.client.accumulators.VoidAccumulatorProvider;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.util.Settings;

import java.util.ArrayList;
import java.util.List;

class ListCollector<T> implements Collector<T>, Context {

  private final Window<?> window = GlobalWindowing.Window.get();
  private final List<T> elements = new ArrayList<>();

  private final AccumulatorProvider accumulatorProvider;

  ListCollector(AccumulatorProvider accumulatorProvider) {
    this.accumulatorProvider = accumulatorProvider;
  }

  @Override
  public void collect(T elem) {
    elements.add(elem);
  }

  @Override
  public Context asContext() {
    return this;
  }

  @Override
  public Window<?> getWindow() {
    return window;
  }

  @Override
  public Counter getCounter(String name) {
    return accumulatorProvider.getCounter(name);
  }

  @Override
  public Histogram getHistogram(String name) {
    return accumulatorProvider.getHistogram(name);
  }

  @Override
  public Timer getTimer(String name) {
    return accumulatorProvider.getTimer(name);
  }

  public List<T> get() {
    return elements;
  }

  public void clear() {
    elements.clear();
  }
}

