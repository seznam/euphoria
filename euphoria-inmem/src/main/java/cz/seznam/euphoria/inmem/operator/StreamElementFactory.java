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
package cz.seznam.euphoria.inmem.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

/**
 * A factory class for {@code StreamElement}s specific to executor.
 */
public interface StreamElementFactory<T> {

  /**
   * Retrieve a stream element that wraps the given data element
   * inside given window and with assigned timestamp.
   * @param element the data element
   * @param window window associated with the element
   * @param stamp timestamp of creation of the element
   * @return {@code StreamElement} instance
   */
  StreamElement<T> data(T element, Window window, long stamp);


  /**
   * Retrieve watermark element.
   * @param stamp stamp of the watermark
   * @return {@code StreamElement} instance
   */
  StreamElement<T> watermark(long stamp);


  /**
   * Retrieve window trigger element.
   * @param window the window to trigger
   * @param stamp timestamp of the trigger
   * @return {@code StreamElement} instance
   */
  StreamElement<T> windowTrigger(Window window, long stamp);

  /**
   * Retrieve end-of-stream element.
   * @return {@code StreamElement} instance
   */
  StreamElement<T> endOfStream();

}
