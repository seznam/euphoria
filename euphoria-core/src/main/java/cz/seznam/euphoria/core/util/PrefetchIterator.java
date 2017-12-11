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
package cz.seznam.euphoria.core.util;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Iterator wrapper allowing to look at next value using {@link #prefetch()}
 * without moving the iterator forward, so following call of {@link #next()}
 * will return the same value.
 */
public class PrefetchIterator<T> implements Iterator<T> {
  private final Iterator<T> it;
  private T prefetched = null;

  public PrefetchIterator(Iterator<T> src) {
    it = src;
  }
  public PrefetchIterator(Stream<T> src) {
    this(src.iterator());
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return prefetched != null || it.hasNext();
  }

  /** {@inheritDoc} */
  @Override
  public T next() {
    if (prefetched != null) {
      T ret = prefetched;
      prefetched = null;
      return ret;
    } else {
      return it.next();
    }
  }

  /**
   * Get next value without moving the iterator forward.
   *
   * @return Next value (or null on the end)
   */
  public T prefetch() {
    if (prefetched == null && it.hasNext())
      prefetched = it.next();
    return prefetched;
  }
}
