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

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.inmem.operator.StreamElement;
import cz.seznam.euphoria.inmem.operator.StreamElementFactory;
import javax.annotation.Nullable;

/**
 * A {@code StreamElement} passed inside {@code KafkaExecutor} pipelines.
 */
class KafkaStreamElement implements StreamElement<Object> {

  static final Factory FACTORY = new Factory();

  static class Factory implements StreamElementFactory<Object> {

    @Override
    public KafkaStreamElement data(Object element, Window window, long stamp) {
      return new KafkaStreamElement(element, window, stamp, Type.ELEMENT);
    }

    @Override
    public KafkaStreamElement watermark(long stamp) {
      return new KafkaStreamElement(null, null, stamp, Type.WATERMARK);
    }

    @Override
    public KafkaStreamElement windowTrigger(Window window, long stamp) {
      return new KafkaStreamElement(null, window, stamp, Type.TRIGGER);
    }

    @Override
    public KafkaStreamElement endOfStream() {
      return new KafkaStreamElement(null, null, Long.MAX_VALUE, Type.EOS);
    }

  }

  private static enum Type {
    ELEMENT,
    WATERMARK,
    TRIGGER,
    EOS
  }


  @Nullable Object element;
  @Nullable Window window;
  int sourcePartition;  
  Type type;
  long stamp;
  
  // FIXME: serialization
  KafkaStreamElement() {
    
  }

  KafkaStreamElement(
      @Nullable Object element,
      @Nullable Window window,
      long stamp,
      Type type,
      int sourcePartition) {

    this.element = element;
    this.window = window;
    this.stamp = stamp;
    this.type = type;
    this.sourcePartition = sourcePartition;
  }

  KafkaStreamElement(
      @Nullable Object element,
      @Nullable Window window,
      long stamp,
      Type type) {

    this(element, window, stamp, type, -1);
  }


  @Override
  public boolean isElement() {
    return type == Type.ELEMENT;
  }

  @Override
  public boolean isEndOfStream() {
    return type == Type.EOS;
  }

  @Override
  public boolean isWatermark() {
    return type == Type.WATERMARK;
  }

  @Override
  public boolean isWindowTrigger() {
    return type == Type.TRIGGER;
  }

  @Override
  public Window getWindow() {
    return window;
  }

  @Override
  public long getTimestamp() {
    return stamp;
  }

  @Override
  public Object getElement() {
    return element;
  }

  @Override
  public String toString() {
    return "KafkaStreamElement("
        + "element=" + element
        + ", window=" + window
        + ", stamp=" + stamp
        + ", type=" + type
        + ")";
  }


  public int getSourcePartition() {
    return sourcePartition;
  }

  void reassignTimestamp(long timestamp) {
    this.stamp = timestamp;
  }

}
