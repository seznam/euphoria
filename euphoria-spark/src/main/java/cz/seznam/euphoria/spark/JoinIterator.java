/*
 * Copyright 2016-2019 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.client.operator.Join;
import org.apache.spark.api.java.Optional;
import cz.seznam.euphoria.shadow.com.google.common.collect.AbstractIterator;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Given an iterator that emits a sequence of tuples as a result of some full-outer join, filters
 * out those tuples that don't conform to a given join-type.
 *
 * <p>In other words, the following are removed:
 *
 * <p>* no entries for a full-outer join, * entries with an empty left-side value for a left join
 * and * entries with an empty right-side value for a right join.
 */
class JoinIterator<K, L, R> extends AbstractIterator<Tuple2<K, Tuple2<Optional<L>, Optional<R>>>> {

  private final Iterator<Tuple2<K, Tuple2<Optional<L>, Optional<R>>>> inner;
  private final Join.Type joinType;

  JoinIterator(Iterator<Tuple2<K, Tuple2<Optional<L>, Optional<R>>>> inner, Join.Type joinType) {
    this.inner = inner;
    this.joinType = joinType;
  }

  @Override
  protected Tuple2<K, Tuple2<Optional<L>, Optional<R>>> computeNext() {
    Tuple2<K, Tuple2<Optional<L>, Optional<R>>> item;
    Tuple2<Optional<L>, Optional<R>> value;
    while (inner.hasNext()) {
      item = inner.next();
      value = item._2;
      switch (joinType) {
        case INNER:
          if (value._1.isPresent() && value._2.isPresent()) {
            return item;
          }
          break;
        case FULL:
          return item;
        case LEFT:
          if (value._1.isPresent()) {
            return item;
          }
          break;
        case RIGHT:
          if (value._2.isPresent()) {
            return item;
          }
          break;
        default:
          throw new AssertionError("Unexpected Join.Type [" + joinType + "].");
      }
    }
    return endOfData();
  }
}
