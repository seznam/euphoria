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
package cz.seznam.euphoria.tez;

import cz.seznam.euphoria.shadow.com.google.common.base.Preconditions;
import cz.seznam.euphoria.shadow.com.google.common.collect.AbstractIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Processor of operator chain that does not require shuffle.
 */
public class ChainProcessor extends SimpleMRProcessor {

  private Chain chain;

  public ChainProcessor(ProcessorContext context) {
    super(context);
  }

  @Override
  public void initialize() throws Exception {
    chain = (Chain) SerializationUtils.fromBytes(
        getContext().getUserPayload().deepCopyAsArray());
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run() throws Exception {

    Preconditions.checkArgument(getInputs().size() > 0, "Need at least one input.");
    final KeyValueReader reader =
        (KeyValueReader) getInputs().values().iterator().next().getReader();

    final Iterator<Object> iterator = new AbstractIterator<Object>() {

      @Override
      protected Object computeNext() {
        try {
          if (!reader.next()) {
            return endOfData();
          }
          return reader.getCurrentValue();
        } catch (IOException e) {
          throw new IllegalStateException("Iterator error.", e);
        }
      }
    };

    // TODO characteristics
    Stream<Object> stream = StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterator, 0), false);

    final List<Chain.StreamModifier<Object, Object>> modifiers = chain.getModifiers();
    for (Chain.StreamModifier<Object, Object> modifier : modifiers) {
      stream = modifier.modifyStream(stream);
    }

    final KeyValueWriter writer = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

    stream.forEach(result -> {
      try {
        writer.write(NullWritable.get(), result);
      } catch (IOException e) {
        throw new RuntimeException("Writer error.", e);
      }
    });
  }

  /**
   * Convert chain to user payload, so we can distribute it with the processor.
   *
   * @param chain to serialize
   * @return user payload
   */
  static UserPayload toUserPayload(Chain<?> chain) {
    try {
      return UserPayload.create(ByteBuffer.wrap(SerializationUtils.toBytes(chain)));
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to serialize user payload.", e);
    }
  }
}
