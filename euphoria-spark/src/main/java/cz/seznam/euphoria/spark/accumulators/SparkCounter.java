/*
 * Copyright 2016-2020 Seznam.cz, a.s.
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
package cz.seznam.euphoria.spark.accumulators;

import cz.seznam.euphoria.core.client.accumulators.Counter;

class SparkCounter implements Counter, SparkAccumulator<Long> {

  private long value = 0;

  @Override
  public void increment(long value) {
    this.value+= value;
  }

  @Override
  public void increment() {
    this.value++;
  }

  @Override
  public Long value() {
    return value;
  }

  @Override
  public SparkAccumulator<Long> merge(SparkAccumulator<Long> other) {
    this.value += other.value();
    return this;
  }

  @Override
  public String toString() {
    return Long.toString(value);
  }
}
