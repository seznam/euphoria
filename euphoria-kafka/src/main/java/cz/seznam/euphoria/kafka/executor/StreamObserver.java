/*
 * Copyright 2016-2017 Seznam.cz, a.s..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.kafka.executor;

import java.util.List;

/**
 * An observer of streaming data.
 */
public interface StreamObserver<T> {

  /**
   * Called when the observer is registered in the observable stream.
   */
  default void onRegistered() {
    
  }

  /**
   * Called each time partitions are assigned or the assignment is changed.
   */
  default void onAssign(List<Integer> partitions) {

  }

  /**
   * Observe next data element.
   */
  void onNext(int partitionId, T elem);

  /**
   * Error occurred on the stream.
   */
  void onError(Throwable err);

  /**
   * The stream has ended.
   */
  void onCompleted();

}
