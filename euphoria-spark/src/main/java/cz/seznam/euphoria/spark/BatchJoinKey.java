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

class BatchJoinKey<KEY> {

  enum Side {
    LEFT,
    RIGHT
  }

  private final KEY key;
  private final Side side;

  BatchJoinKey(KEY key, Side side) {
    this.key = key;
    this.side = side;
  }

  KEY getKey() {
    return key;
  }

  Side getSide() {
    return side;
  }
}
