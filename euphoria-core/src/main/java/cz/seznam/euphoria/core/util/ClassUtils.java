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
package cz.seznam.euphoria.core.util;

import javax.annotation.Nullable;

public class ClassUtils {

  /**
   * Find out whether class implements {@link Comparable} or not.
   *
   * @param clazz to analyze
   * @return implements comparable
   */
  public static boolean isComparable(@Nullable Class<?> clazz) {
    return clazz != null && Comparable.class.isAssignableFrom(clazz);
  }
}
