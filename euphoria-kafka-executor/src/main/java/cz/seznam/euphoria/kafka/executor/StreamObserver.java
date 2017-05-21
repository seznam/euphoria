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
 * An observer of streaming data.
 */
public interface StreamObserver<T> {

  /**
   * Called each time partitions are assigned or the assignment is changed.
   * @param numPartitions total number of partitions assigned to this observer
   * Each observer will retrieve elements fro partitions (0, numPartitions - 1).
   * These partitions do not necessarily correspond to any physical partitions
   * numbering of partitions (e.g. inside Kafka).
   */
  default void onAssign(int numPartitions) {

  }

  /**
   * Observe next data element.
   * @param partitionId ID of partition of the input element (zero based)
   * @param elem the input element
   */
  void onNext(int partitionId, T elem);

  /**
   * Error occurred on the stream.
   * @param err the error thrown during observing of the stream
   */
  void onError(Throwable err);

  /**
   * The stream has ended.
   */
  void onCompleted();

}
