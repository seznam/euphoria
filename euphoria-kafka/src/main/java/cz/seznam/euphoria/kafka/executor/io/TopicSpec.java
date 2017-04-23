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
package cz.seznam.euphoria.kafka.executor.io;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

/**
 * A specification of the topic that will serve as
 * exchange topic for repartitions.
 */
public class TopicSpec<W extends Window, T> {

  /**
   * Create new topic specification.
   * @param name name of the topic
   * @param windowSerialization serialization to use to serialize window of
   * an element
   * @param payloadSerialization serialization to use to serialize value of
   * an element
   * @return the topic specification
   */
  public static <W extends Window, T> TopicSpec<W, T> of(
      String name,
      Serde<W> windowSerialization,
      Serde<T> payloadSerialization) {
    
    return new TopicSpec(name, windowSerialization, payloadSerialization);
  }

  private final String name;
  private final Serde<W> window;
  private final Serde<T> payload;

  private TopicSpec(String name, Serde<W> window, Serde<T> payload) {
    this.name = name;
    this.window = window;
    this.payload = payload;
  }

  /**
   * Retrieve name of the topic.
   * @returns name of the topic
   * @return
   */
  public String getName() {
    return name;
  }

  /**
   * Retrieve serialization for windows.
   * @return the serialization for windows
   */
  public Serde<W> getWindowSerialization() {
    return window;
  }
  /**
   * Retrieve serialization for payloads.
   * @return the serialization for payloads
   */
  public Serde<T> getPayloadSerialization() {
    return payload;
  }

}
