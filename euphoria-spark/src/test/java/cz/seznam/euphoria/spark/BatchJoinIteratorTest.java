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

import cz.seznam.euphoria.core.client.util.Either;
import org.apache.spark.api.java.Optional;
import cz.seznam.euphoria.shadow.com.google.common.collect.Lists;
import org.junit.Test;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static cz.seznam.euphoria.spark.BatchJoinKey.Side.LEFT;
import static cz.seznam.euphoria.spark.BatchJoinKey.Side.RIGHT;

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
    return new Tuple2<>(new BatchJoinKey<>(key, side, 0), value);
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
