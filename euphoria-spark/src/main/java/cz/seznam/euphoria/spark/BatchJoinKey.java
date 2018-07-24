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

import cz.seznam.euphoria.shadow.com.google.common.base.Preconditions;

import javax.annotation.Nonnull;

class BatchJoinKey<KEY> implements Comparable<BatchJoinKey<KEY>> {

  enum Side {
    LEFT,
    RIGHT
  }

  private final KEY key;
  private final Side side;
  private final int partitionIdx;

  BatchJoinKey(KEY key, Side side, int partitionIdx) {
    Preconditions.checkArgument(key instanceof Comparable, "Key is not comparable.");
    this.key = key;
    this.side = side;
    this.partitionIdx = partitionIdx;
  }

  KEY getKey() {
    return key;
  }

  Side getSide() {
    return side;
  }

  int getPartitionIdx() {
    return partitionIdx;
  }

  @Override
  @SuppressWarnings("unchecked")
  public int compareTo(@Nonnull BatchJoinKey<KEY> other) {
    int result = ((Comparable) getKey()).compareTo(other.getKey());
    if (result == 0) {
      return compareSide(getSide(), other.getSide());
    }
    return result;
  }

  private static int compareSide(Side a, Side b) {
    // ~ LEFT side precedes RIGHT side
    if (a.equals(b)) {
      return 0;
    }
    if (a.equals(Side.LEFT)) {
      return -1;
    }
    return 1;

  }

  @Override
  public String toString() {
    return "BatchJoinKey{" +
        "key=" + key +
        ", side=" + side +
        ", partitionIdx=" + partitionIdx +
        '}';
  }
}
