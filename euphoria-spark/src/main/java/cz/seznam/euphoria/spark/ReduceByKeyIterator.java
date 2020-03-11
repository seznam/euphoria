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
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.shadow.com.google.common.collect.AbstractIterator;
import cz.seznam.euphoria.shadow.com.google.common.collect.Iterators;
import cz.seznam.euphoria.shadow.com.google.common.collect.PeekingIterator;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Transform stream of sorted key values into stream of value iterators for each key. This iterator
 * can be iterated only once!
 *
 * @param <KEY> type of key iterator emits
 * @param <VALUE> type of value iterator emits
 */
class ReduceByKeyIterator<KEY, VALUE> extends AbstractIterator<Tuple2<KEY, Iterable<VALUE>>> {

  private final PeekingIterator<Tuple2<KEY, VALUE>> inner;

  private KEY previousKey;

  ReduceByKeyIterator(Iterator<Tuple2<KEY, VALUE>> inner) {
    this.inner = Iterators.peekingIterator(inner);
  }

  @Override
  protected Tuple2<KEY, Iterable<VALUE>> computeNext() {
    while (inner.hasNext()) {
      // just peek, so the value iterator can see the first value
      final Tuple2<KEY, VALUE> item = inner.peek();
      final KEY currentKey = item._1;

      if (currentKey.equals(previousKey)) {
        // inner iterator did not consume all values for a given key, we need to skip ahead until we
        // find value for the next key
        inner.next();
        continue;
      }

      previousKey = currentKey;

      return new Tuple2<>(
          previousKey,
          () ->
              new AbstractIterator<VALUE>() {

                @Override
                protected VALUE computeNext() {
                  // compute until we know, that next element contains a new key or there are no
                  // more elements to process
                  if (inner.hasNext() && currentKey.equals(inner.peek()._1)) {
                    return inner.next()._2;
                  }
                  return endOfData();
                }
              });
    }
    return endOfData();
  }
}
