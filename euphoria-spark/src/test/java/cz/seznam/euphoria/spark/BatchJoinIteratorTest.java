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
package cz.seznam.euphoria.spark;

import static cz.seznam.euphoria.spark.BatchJoinKey.Side.LEFT;
import static cz.seznam.euphoria.spark.BatchJoinKey.Side.RIGHT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.shadow.com.google.common.collect.Lists;
import cz.seznam.euphoria.spark.BatchJoinIterator.StatsItem;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.api.java.Optional;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

public class BatchJoinIteratorTest {

  @Test
  public void joinsLeftAndRightSides() {

    final Iterator<Tuple2<BatchJoinKey<String>, Either<String, String>>> inner =
        asList(
                entry("key1", LEFT, Either.left("v1")),
                entry("key1", RIGHT, Either.right("w1")),
                entry("key1", RIGHT, Either.right("w2")),
                entry("key2", LEFT, Either.left("v1")),
                entry("key2", RIGHT, Either.right("w1")))
            .iterator();

    final List<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>> results =
        Lists.newArrayList(new BatchJoinIterator<>(inner));

    assertEquals(
        asList(entry("key1", "v1", "w1"), entry("key1", "v1", "w2"), entry("key2", "v1", "w1")),
        results);
  }

  @Test
  public void emitCartesianProduct() {

    final Iterator<Tuple2<BatchJoinKey<String>, Either<String, String>>> inner =
        asList(
                entry("key1", LEFT, Either.left("v1")),
                entry("key1", LEFT, Either.left("v2")),
                entry("key1", RIGHT, Either.right("w1")),
                entry("key1", RIGHT, Either.right("w2")),
                entry("key1", RIGHT, Either.right("w3")))
            .iterator();

    final List<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>> results =
        Lists.newArrayList(new BatchJoinIterator<>(inner));

    assertEquals(
        asList(
            entry("key1", "v1", "w1"),
            entry("key1", "v2", "w1"),
            entry("key1", "v1", "w2"),
            entry("key1", "v2", "w2"),
            entry("key1", "v1", "w3"),
            entry("key1", "v2", "w3")),
        results);
  }

  @Test
  public void testStatistics() {

    final Iterator<Tuple2<BatchJoinKey<String>, Either<String, String>>> inner =
        asList(
            entry("key1", LEFT, Either.left("v1")),
            entry("key1", LEFT, Either.left("v2")),
            entry("key1", RIGHT, Either.right("w1")),
            entry("key1", RIGHT, Either.right("w2")),
            entry("key1", RIGHT, Either.right("w3")))
            .iterator();

    BatchJoinIterator<String, String, String> iteratorUnderTest = new BatchJoinIterator<>(inner);
    final List<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>> results =
        Lists.newArrayList(iteratorUnderTest);

    assertEquals(
        asList(
            entry("key1", "v1", "w1"),
            entry("key1", "v2", "w1"),
            entry("key1", "v1", "w2"),
            entry("key1", "v2", "w2"),
            entry("key1", "v1", "w3"),
            entry("key1", "v2", "w3")),
        results);

    PriorityQueue<StatsItem<String>> topKeys = iteratorUnderTest.topKeys;

    Assert.assertEquals(1, topKeys.size());

    StatsItem<String> item = topKeys.peek();
    Assert.assertNotNull(item);

    Assert.assertEquals(2L, item.leftSideElements);
    Assert.assertEquals(3L, item.rightSideElements);
  }

  @Test
  public void testStatisticsManyKeys() { //TODO add more than 10 keys test

    final List<Tuple2<BatchJoinKey<String>, Either<String, String>>> inputs = IntStream
        .range(1, 25)
        .mapToObj(i -> entry("key" + i, LEFT, Either.left("vl")))
        .collect(Collectors.toList());

    inputs.add(entry("key24", RIGHT, Either.right("vr1")));
    inputs.add(entry("key24", RIGHT, Either.right("vr2")));

    inputs.add(entry("key25", LEFT, Either.left("vl")));
    inputs.add(entry("key25", RIGHT, Either.right("vr1")));
    inputs.add(entry("key25", RIGHT, Either.right("vr2")));
    inputs.add(entry("key25", RIGHT, Either.right("vr3")));

    System.out.println(inputs);

    BatchJoinIterator<String, String, String> iteratorUnderTest =
        new BatchJoinIterator<>(inputs.iterator());
    final List<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>> results =
        Lists.newArrayList(iteratorUnderTest);


    PriorityQueue<StatsItem<String>> topKeys = iteratorUnderTest.topKeys;

    Assert.assertEquals(10, topKeys.size());
    Deque<StatsItem<String>> biggestKeyFirst = new ArrayDeque<>(topKeys.size());
    StatsItem<String> topItem;
    while((topItem = topKeys.poll()) != null){
      biggestKeyFirst.addFirst(topItem);
    }

    StatsItem<String> biggest = biggestKeyFirst.poll();
    Assert.assertNotNull(biggest);
    Assert.assertEquals("key25", biggest.key);
    Assert.assertEquals(1, biggest.leftSideElements);
    Assert.assertEquals(3, biggest.rightSideElements);

    StatsItem<String> secondBiggest = biggestKeyFirst.poll();
    Assert.assertNotNull(biggest);
    Assert.assertEquals("key24", secondBiggest.key);
    Assert.assertEquals(1, secondBiggest.leftSideElements);
    Assert.assertEquals(2, secondBiggest.rightSideElements);

    biggestKeyFirst.forEach( (item) -> {
      Assert.assertEquals(1, item.leftSideElements);
      Assert.assertEquals(0, item.rightSideElements);
    });

  }

  @Test
  public void emitsALeftLonerWhenAConsecutiveSecondLeftSideComes() {

    final Iterator<Tuple2<BatchJoinKey<String>, Either<String, String>>> inner =
        asList(
                entry("key1", LEFT, Either.left("v1")),
                entry("key1", RIGHT, Either.right("w1")),
                // ~ a left side with no right sides
                entry("key2", LEFT, Either.left("v1")),
                // ~ a consecutive second left side
                entry("key3", LEFT, Either.left("v1")),
                entry("key3", RIGHT, Either.right("w1")))
            .iterator();

    final List<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>> results =
        Lists.newArrayList(new BatchJoinIterator<>(inner));

    assertEquals(
        asList(
            entry("key1", "v1", "w1"),
            entry("key2", "v1", Optional.empty()),
            entry("key3", "v1", "w1")),
        results);
  }

  @Test
  public void emitsARightLonerWhenAConsecutiveSecondLeftSideComes() {

    final Iterator<Tuple2<BatchJoinKey<String>, Either<String, String>>> inner =
        asList(
                entry("key1", LEFT, Either.left("v1")),
                entry("key1", RIGHT, Either.right("w1")),
                // ~ a left side with no right sides
                entry("key2", RIGHT, Either.right("w1")),
                // ~ a consecutive second left side
                entry("key3", LEFT, Either.left("v1")),
                entry("key3", RIGHT, Either.right("w1")))
            .iterator();

    final List<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>> results =
        Lists.newArrayList(new BatchJoinIterator<>(inner));

    assertEquals(
        asList(
            entry("key1", "v1", "w1"),
            entry("key2", Optional.empty(), "w1"),
            entry("key3", "v1", "w1")),
        results);
  }

  @Test
  public void emitsALeftLonerAndARightLonerWhenARightSideWithADifferentKeyComes() {

    final Iterator<Tuple2<BatchJoinKey<String>, Either<String, String>>> inner =
        asList(
                entry("key1", LEFT, Either.left("v1")),
                entry("key1", RIGHT, Either.right("w1")),
                // ~ a left side with no right sides
                entry("key2", LEFT, Either.left("v1")),
                // ~ a right side with a different key
                entry("key3", RIGHT, Either.right("w1")))
            .iterator();

    final List<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>> results =
        Lists.newArrayList(new BatchJoinIterator<>(inner));

    assertEquals(
        asList(
            entry("key1", "v1", "w1"),
            entry("key3", Optional.empty(), "w1"),
            entry("key2", "v1", Optional.empty())),
        results);
  }

  @Test
  public void emitsARightLonerWhenARightSideWithADifferentKeyComes() {

    final Iterator<Tuple2<BatchJoinKey<String>, Either<String, String>>> inner =
        asList(
                entry("key1", LEFT, Either.left("v1")),
                entry("key1", RIGHT, Either.right("w1")),
                // ~ a right side with a different key
                entry("key3", RIGHT, Either.right("w1")))
            .iterator();

    final List<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>> results =
        Lists.newArrayList(new BatchJoinIterator<>(inner));

    assertEquals(asList(entry("key1", "v1", "w1"), entry("key3", Optional.empty(), "w1")), results);
  }

  @Test
  public void lonerLeftSideInTheEnd() {

    final Iterator<Tuple2<BatchJoinKey<String>, Either<String, String>>> inner =
        asList(
                entry("key1", LEFT, Either.left("v1")),
                entry("key1", RIGHT, Either.right("w1")),
                entry("key1", RIGHT, Either.right("w2")),
                entry("key2", LEFT, Either.left("v1")))
            .iterator();

    final List<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>> results =
        Lists.newArrayList(new BatchJoinIterator<>(inner));

    assertEquals(
        asList(
            entry("key1", "v1", "w1"),
            entry("key1", "v1", "w2"),
            entry("key2", "v1", Optional.empty())),
        results);
  }

  @Test
  public void lonerRightSideInTheEnd() {

    final Iterator<Tuple2<BatchJoinKey<String>, Either<String, String>>> inner =
        asList(
                entry("key1", LEFT, Either.left("v1")),
                entry("key1", RIGHT, Either.right("w1")),
                entry("key1", RIGHT, Either.right("w2")),
                entry("key2", RIGHT, Either.right("w1")))
            .iterator();

    final List<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>> results =
        Lists.newArrayList(new BatchJoinIterator<>(inner));

    assertEquals(
        asList(
            entry("key1", "v1", "w1"),
            entry("key1", "v1", "w2"),
            entry("key2", Optional.empty(), "w1")),
        results);
  }

  private Tuple2<BatchJoinKey<String>, Either<String, String>> entry(
      String key, BatchJoinKey.Side side, Either<String, String> value) {
    return new Tuple2<>(new BatchJoinKey<>(key, side), value);
  }

  private Tuple2<String, Tuple2<Optional<String>, Optional<String>>> entry(
      String key, String leftSide, String rightSide) {
    return new Tuple2<>(key, new Tuple2<>(Optional.of(leftSide), Optional.of(rightSide)));
  }

  private Tuple2<String, Tuple2<Optional<String>, Optional<String>>> entry(
      String key, String leftSide, Optional<String> rightSide) {
    return new Tuple2<>(key, new Tuple2<>(Optional.of(leftSide), rightSide));
  }

  private Tuple2<String, Tuple2<Optional<String>, Optional<String>>> entry(
      String key, Optional<String> leftSide, String rightSide) {
    return new Tuple2<>(key, new Tuple2<>(leftSide, Optional.of(rightSide)));
  }
}
