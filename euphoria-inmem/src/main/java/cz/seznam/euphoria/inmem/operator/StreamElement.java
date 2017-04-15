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

package cz.seznam.euphoria.inmem.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;

/**
 * An interface flowing in pipelines of generic executor.
 */
public interface StreamElement<T> extends WindowedElement<Window, T> {

  /**
   * Check if this regular element message.
   * @return {@code true} if this is data element, {@code false} otherwise
   */
  boolean isElement();

  /**
   * Check if this is an end-of-stream message.
   * @return true if this element is end of stream
   */
  boolean isEndOfStream();

  /**
   * Check if this is watermark.
   * @return true if this is watermark
   */
  boolean isWatermark();

  /**
   * Check if this is window trigger event.
   * @return true if this is window trigger
   */
  boolean isWindowTrigger();

}
