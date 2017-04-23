/*
 * Copyright 2017 Seznam.cz, a.s..
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

/**
 * Class combining serializer and deserializer together.
 */
public interface Serde<T> {

  /**
   * Serializer.
   * @param <T> the type parameter to serialize
   */
  @FunctionalInterface
  public static interface Serializer<T> {

    /**
     * Serialize element.
     * @param elem the element to serialize
     * @return serialized bytes
     */
    byte[] apply(T elem);

  }

  /**
   * Deserializer.
   * @param <T> the type parameter to deserialize
   */
  @FunctionalInterface
  public static interface Deserializer<T> {

    /**
     * Deserialize element.
     * @param bytes the serialized bytes
     * @return the deserialized instance
     */
    T apply(byte[] bytes);
    
  }

  /**
   * Create {@code Serde} from serializer and deserializer.
   */
  static <T> Serde<T> from(Serializer<T> serializer, Deserializer<T> deserialzier) {
    return new Serde<T>() {
      @Override
      public Serializer<T> serializer() {
        return serializer;
      }

      @Override
      public Deserializer<T> deserializer() {
        return deserialzier;
      }
    };
  }


  /**
   * Retrieve serializer for the type.
   * @return the serializer
   */
  Serializer<T> serializer();

  /**
   * Retrieve deserializer for the type.
   * @return the deserializer
   */
  Deserializer<T> deserializer();

}
