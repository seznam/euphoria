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

/**
 * Interface for writing output to kafka.
 * This interface is provided for the sake of unit testing.
 */
@FunctionalInterface
interface OutputWriter {

  /**
   * Write element to output.
   * @param elem the element to write
   * @param sourcePartition the partition this element originally belonged to
   * @param targetPartition the partition that the element should be sent to
   * @param callback the callback to confirm after writing of the element to
   *                 output partition
   */
  void write(
      KafkaStreamElement elem,
      int sourcePartition,
      int targetPartition,
      Callback callback);
  
}
