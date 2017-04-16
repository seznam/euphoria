/*
 * Copyright 2017 Seznam.cz, a.s..
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

import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.operator.test.AllOperatorsSuite;
import cz.seznam.euphoria.operator.test.CountByKeyTest;
import cz.seznam.euphoria.operator.test.DistinctTest;
import cz.seznam.euphoria.operator.test.FilterTest;
import cz.seznam.euphoria.operator.test.FlatMapTest;
import cz.seznam.euphoria.operator.test.JoinTest;
import cz.seznam.euphoria.operator.test.JoinWindowEnforcementTest;
import cz.seznam.euphoria.operator.test.MapElementsTest;
import cz.seznam.euphoria.operator.test.ReduceByKeyTest;
import cz.seznam.euphoria.operator.test.ReduceStateByKeyTest;
import cz.seznam.euphoria.operator.test.RepartitionTest;
import cz.seznam.euphoria.operator.test.SumByKeyTest;
import cz.seznam.euphoria.operator.test.TopPerKeyTest;
import cz.seznam.euphoria.operator.test.UnionTest;
import cz.seznam.euphoria.operator.test.WindowingTest;
import cz.seznam.euphoria.operator.test.junit.ExecutorEnvironment;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.runners.Suite;

/**
 * Use operator test kit to test the {@code KafkaExecutor} compliance with
 * Euphoria operators semantics.
 */
@Suite.SuiteClasses({
    CountByKeyTest.class,
    DistinctTest.class,
    FilterTest.class,
    FlatMapTest.class,
    JoinTest.class,
    JoinWindowEnforcementTest.class,
    MapElementsTest.class,
    ReduceByKeyTest.class,
    ReduceStateByKeyTest.class,
    RepartitionTest.class,
    SumByKeyTest.class,
    TopPerKeyTest.class,
    UnionTest.class,
    WindowingTest.class,
})
public class KafkaOperatorsTest extends AllOperatorsSuite {

  @Override
  public ExecutorEnvironment newExecutorEnvironment() throws Exception {
    return new ExecutorEnvironment() {
      ExecutorService service = Executors.newCachedThreadPool();
      @Override
      public Executor getExecutor() {

        return new TestKafkaExecutor(service);
      }

      @Override
      public void shutdown() throws Exception {
        service.shutdownNow();
      }

    };
  }

}
