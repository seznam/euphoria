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

/**
 * A stream that can be observed by {@code StreamObserver}.
 */
public interface ObservableStream<T> {

  /**
   * Start to observe the stream.
   * The observer will receive elements from all partitions of this
   * stream.
   * Each observer has a name, when there exist multiple observers
   * with the same name, then the stream is load balanced between them.
   * Observers with different names see the same data.
   * @param name name of the observer
   * @param observer the observer
   */
  void observe(String name, StreamObserver<T> observer);


  /**
   * Retrieve number of partitions of the stream.
   * @return number of partitions of the stream.
   */
  int size();

}
