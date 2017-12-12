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

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

public class PrefetchIteratorTest {

  @Test
  public void testEmpty() {
    List<Integer> list = Collections.emptyList();
    PrefetchIterator<Integer> it = new PrefetchIterator<>(list.iterator());

    assertNull(it.prefetch());
    assertFalse(it.hasNext());
  }

  @Test
  public void testSingle() {
    List<Integer> list = Collections.singletonList(5);
    PrefetchIterator<Integer> it = new PrefetchIterator<>(list.iterator());

    assertEquals(Integer.valueOf(5), it.prefetch());
    assertTrue(it.hasNext());
    assertEquals(Integer.valueOf(5), it.next());
  }

  @Test
  public void testLong() {
    List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
    PrefetchIterator<Integer> it = new PrefetchIterator<>(list.iterator());

    for (Integer expected: list) {
      assertEquals(expected, it.prefetch());
      assertTrue(it.hasNext());
      assertEquals(expected, it.next());
    }

    assertFalse(it.hasNext());
  }
}

