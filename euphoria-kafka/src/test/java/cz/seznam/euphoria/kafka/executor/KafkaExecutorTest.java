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

package cz.seznam.euphoria.kafka.executor;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.CountByKey;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.SumByKey;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test suite for {@code KafkaExecutor}.
 * This includes some basic and rough tests for the executor.
 * More tests are included in {@code KafkaOperatorsTest}, which uses
 * the standard operator test kit.
 */
public class KafkaExecutorTest {

  ExecutorService service;
  KafkaExecutor executor;
  Flow flow;

  @Before
  public void setUp() {
    service = Executors.newCachedThreadPool();
    executor = new TestKafkaExecutor(service);
    flow = Flow.create();
  }

  @After
  public void tearDown() {
    executor.shutdown();
  }

  @Test
  public void testSimpleMapWithTwoPartitions() throws Exception {
    ListDataSource<Integer> source = ListDataSource.bounded(
        Arrays.asList(1, 2, 3),
        Arrays.asList(4, 5, 6));
    ListDataSink<Integer> sink = ListDataSink.get(2);
    Dataset<Integer> input = flow.createInput(source);
    MapElements
        .of(input)
        .using(a -> a + 1)
        .output()
        .persist(sink);
    executor.submit(flow).get();

    assertEquals(2, sink.getOutputs().size());
    assertEquals(Arrays.asList(2, 3, 4), sink.getOutput(0));
    assertEquals(Arrays.asList(5, 6, 7),sink.getOutput(1));
  }

  @Test
  public void testFlatMapWithTwoPartitions() throws Exception {
    ListDataSource<Integer> source = ListDataSource.bounded(
        Arrays.asList(1, 2, 3),
        Arrays.asList(4, 5, 6));
    ListDataSink<Integer> sink = ListDataSink.get(2);
    Dataset<Integer> input = flow.createInput(source);
    FlatMap
        .of(input)
        .using((Integer a, Context<Integer> c) -> {
          for (int i = 0; i < a; i++) {
            c.collect(a);
          }
        })
        .output()
        .persist(sink);

    executor.submit(flow).get();

    assertEquals(2, sink.getOutputs().size());
    assertEquals(Arrays.asList(1, 2, 2, 3, 3, 3), sink.getOutput(0));
    assertEquals(Arrays.asList(4, 4, 4, 4, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6),
        sink.getOutput(1));

  }


  @Test
  public void testRepartitionWithTwoPartitions() throws Exception {
    ListDataSource<Integer> source = ListDataSource.bounded(
        Arrays.asList(1, 2, 3),
        Arrays.asList(4, 5, 6));
    ListDataSink<Integer> sink = ListDataSink.get(2);
    Dataset<Integer> input = flow.createInput(source);
    Repartition
        .of(input)
        .setPartitioner(a -> a % 2)
        .output()
        .persist(sink);

    executor.submit(flow).get();

    assertEquals(2, sink.getOutputs().size());
    assertEquals(Sets.newHashSet(2, 4, 6), Sets.newHashSet(sink.getOutput(0)));
    assertEquals(Sets.newHashSet(1, 3, 5), Sets.newHashSet(sink.getOutput(1)));

  }

  @Test
  public void testRepartitionWithTwoPartitionsOneInput() throws Exception {
    ListDataSource<Integer> source = ListDataSource.bounded(
        Arrays.asList(1, 2, 3, 4, 5, 6));
    ListDataSink<Integer> sink = ListDataSink.get(2);
    Dataset<Integer> input = flow.createInput(source);
    Repartition
        .of(input)
        .setPartitioner(a -> a % 2)
        .setNumPartitions(2)
        .output()
        .persist(sink);

    executor.submit(flow).get();

    assertEquals(2, sink.getOutputs().size());
    assertEquals(Arrays.asList(2, 4, 6), sink.getOutput(0));
    assertEquals(Arrays.asList(1, 3, 5), sink.getOutput(1));
  }

  @Test
  public void testUnionSamePartitioning() throws Exception {
    ListDataSource<Integer> first = ListDataSource.bounded(
        Arrays.asList(1, 2),
        Arrays.asList(3));
    ListDataSource<Integer> second = ListDataSource.bounded(
        Arrays.asList(4, 5, 6),
        Arrays.asList(7, 8, 9));
    ListDataSink<Integer> sink = ListDataSink.get(2);
    Union.of(flow.createInput(first), flow.createInput(second))
        .output()
        .persist(sink);

    executor.submit(flow).get();

    assertEquals(2, sink.getOutputs().size());
    assertEquals(
        Sets.newHashSet(1, 2, 4, 5, 6),
        Sets.newHashSet(sink.getOutput(0)));
    
    assertEquals(
        Sets.newHashSet(3, 7, 8, 9),
        Sets.newHashSet(sink.getOutput(1)));
  }

  @Test
  @Ignore("Ignored until resolving of issue #92")
  public void testUnionDifferentPartitioning() throws Exception {
    ListDataSource<Integer> first = ListDataSource.bounded(
        Arrays.asList(1, 2, 3));
    ListDataSource<Integer> second = ListDataSource.bounded(
        Arrays.asList(4, 5, 6));
    ListDataSink<Integer> sink = ListDataSink.get(2);
    Union.of(flow.createInput(first), flow.createInput(second))
        .output()
        .persist(sink);

    executor.submit(flow).get();

    assertEquals(2, sink.getOutputs().size());
    assertEquals(
        Sets.newHashSet(1, 2, 3, 4, 5, 6),
        Sets.newHashSet(sink.getOutput(0)));

    assertEquals(
        Sets.newHashSet(7, 8, 9),
        Sets.newHashSet(sink.getOutput(1)));
  }


  @Test
  public void testReduceByKey() throws Exception {
    ListDataSource<Integer> source = ListDataSource.bounded(
        Arrays.asList(1, 2, 3),
        Arrays.asList(4, 5, 6));
    ListDataSink<Pair<Integer, Integer>> sink = ListDataSink.get(2);
    Dataset<Integer> input = flow.createInput(source);
    ReduceByKey
        .of(input)
        .keyBy(a -> a % 2)
        .combineBy(Sums.ofInts())
        .setPartitioner(a -> a % 2)
        .output()
        .persist(sink);

    executor.submit(flow).get();

    assertEquals(2, sink.getOutputs().size());
    assertEquals(Arrays.asList(Pair.of(0, 12)), sink.getOutput(0));
    assertEquals(Arrays.asList(Pair.of(1, 9)), sink.getOutput(1));
  }

  @Test
  public void testCountByKey() throws Exception {
    ListDataSource<Integer> source = ListDataSource.bounded(
        Arrays.asList(1, 2, 3),
        Arrays.asList(4, 5, 6));
    ListDataSink<Pair<Integer, Long>> sink = ListDataSink.get(2);
    Dataset<Integer> input = flow.createInput(source);
    CountByKey
        .of(input)
        .keyBy(a -> a % 2)
        .setNumPartitions(1)
        .output()
        .persist(sink);

    executor.submit(flow).get();

    assertEquals(2, sink.getOutputs().size());
    assertEquals(Arrays.asList(Pair.of(0, 3L), Pair.of(1, 3L)), sink.getOutput(0));
  }

  @Test
  public void testReduceByKeyWindowedWithEventTime() throws Exception {
    ListDataSource<Integer> source = ListDataSource.bounded(
        Arrays.asList(1, 3, 6),
        Arrays.asList(1, 3, 6));
    ListDataSink<Pair<Integer, Integer>> sink = ListDataSink.get(2);
    Dataset<Integer> input = flow.createInput(source);
    ReduceByKey
        .of(input)
        .keyBy(a -> a % 2)
        .combineBy(Sums.ofInts())
        .setPartitioner(a -> a % 2)
        .windowBy(Time.of(Duration.ofSeconds(1)), a -> 2000L * a)
        .output()
        .persist(sink);

    executor.submit(flow).get();

    assertEquals(2, sink.getOutputs().size());
    assertEquals(Arrays.asList(Pair.of(0, 12)), sink.getOutput(0));
    assertEquals(Arrays.asList(Pair.of(1, 2), Pair.of(1, 6)), sink.getOutput(1));
  }

  @Test
  public void testDistinctStream() throws Exception {
    ListDataSource<Pair<Integer, Long>> input = ListDataSource.unbounded(
        asTimedList(100, 1, 2, 3, 3, 2, 1),
        asTimedList(100, 1, 2, 3, 3, 2, 1)
    );

    ListDataSink<Integer> sink = ListDataSink.get(2);

    Distinct.of(flow.createInput(input))
        .mapped(Pair::getFirst)
        .setNumPartitions(2)
        .setPartitioner(e -> e)
        .windowBy(Time.of(Duration.ofSeconds(1)), Pair::getSecond)
        .output()
        .persist(sink);

    executor.submit(flow).get(1, TimeUnit.SECONDS);

    assertEquals(Arrays.asList(2), sink.getOutput(0));
    assertEquals(Sets.newHashSet(1, 3), Sets.newHashSet(sink.getOutput(1)));

  }


  @Test
  public void testSumByKey() throws Exception {

    ListDataSource<Integer> input = ListDataSource.unbounded(
        Arrays.asList(1, 2, 3, 4, 5),
        Arrays.asList(6, 7, 8, 9)
    );

    ListDataSink<Pair<Integer, Long>> sink = ListDataSink.get(2);

    SumByKey.of(flow.createInput(input))
            .keyBy(e -> e % 2)
            .valueBy(e -> (long) e)
            .setPartitioner(e -> e % 2)
            .windowBy(Time.of(Duration.ofSeconds(1)))
            .output()
            .persist(sink);

    executor.submit(flow).get(2, TimeUnit.SECONDS);

    assertEquals(Arrays.asList(Pair.of(0, 20L)), sink.getOutput(0));
    assertEquals(Arrays.asList(Pair.of(1, 25L)), sink.getOutput(1));

  }

  private List<Pair<Integer, Long>> asTimedList(long step, Integer ... values) {
    List<Pair<Integer, Long>> ret = new ArrayList<>(values.length);
    long i = step;
    for (Integer v : values) {
      ret.add(Pair.of(v, i));
      i += step;
    }
    return ret;
  }


}
