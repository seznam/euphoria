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

import cz.seznam.euphoria.core.client.io.Collector;
import org.junit.Test;

import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class ConfigAwareTest {

  private ConfigAware<SimpleConfig> configAware = new ConfigAware<>(new SimpleConfig());
  private int inputValue = 10;

  @Test
  public void configAwareUnaryFunctionTest() {
    int additionWithConfig = configAware.withConfig(
        (Integer input, SimpleConfig simpleConf) -> input + simpleConf.value
    ).apply(inputValue);

    assertEquals(20, additionWithConfig);
  }

  @Test
  public void configAwareBinaryFunctorTest() {
    int secondInputValue = 1;
    configAware.withConfig(
        (Integer left, Integer right, Collector<Integer> ctx, SimpleConfig simpleConfig) ->
            assertEquals(21, left + right + simpleConfig.value)
    ).apply(inputValue, secondInputValue, null);
  }

  @Test
  public void configAwareUnaryFunctorTest() {
    configAware.withConfig(
        (Integer left, Collector<Integer> ctx, SimpleConfig simpleConfig) ->
            assertEquals(20, left + simpleConfig.value)
    ).apply(inputValue, null);
  }

  @Test
  public void configAwareReduceFunctorTest() {
    ConfigAware.ConfigAwareReduceFunctor<Integer, Integer, SimpleConfig> reduceFunctor =
        (Stream<Integer> stream, Collector<Integer> ctx, SimpleConfig simpleConfig) ->
            assertEquals(16, stream.mapToInt(Integer::intValue).sum() + simpleConfig.value);

    configAware.withConfig(reduceFunctor).apply(Stream.of(1, 2, 3), null);
  }

  class SimpleConfig implements ConfigAware.Config {
    int value = 10;
  }
}
