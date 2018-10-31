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
package cz.seznam.euphoria.beam.io;

import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import cz.seznam.euphoria.core.client.io.BoundedReader;
import cz.seznam.euphoria.core.client.io.DataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * EuphoriaIO is read connector to Beam where DataSource is transformed to PCollection.
 * It's useful for smoother transition between Euphoria and Beamphoria
 *
 * E.g.:
 * {@code
 * PCollection<KV<Text, ImmutableBytesWritable>> input =
 *    pipeline.apply(EuphoriaIO.read(dataSource, kvCoderTextBytes));
 * }
 */
public class EuphoriaIO {

  public static <T> Read<T> read(DataSource<T> dataSource, Coder<T> outputCoder) {
    return new Read<>(dataSource, outputCoder);
  }

  public static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    private final DataSource<T> source;
    private final Coder<T> outputCoder;

    Read(DataSource<T> source, Coder<T> outputCoder) {
      this.source = source;
      this.outputCoder = outputCoder;
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      if (source.isBounded()) {
        org.apache.beam.sdk.io.Read.Bounded<T> bounded =
            org.apache.beam.sdk.io.Read.from(new BoundedSourceWrapper<>(source.asBounded(), this));

        return input.apply(bounded);
      } else {
        throw new UnsupportedOperationException("Unbounded is not supported for now.");
      }
    }

    public Coder<T> getOutputCoder() {
      return outputCoder;
    }
  }

  public static class BoundedSourceWrapper<T> extends BoundedSource<T> {

    private final BoundedDataSource<T> source;
    private final Read<T> readInstance;

    public BoundedSourceWrapper(BoundedDataSource<T> source, Read<T> readInstance) {
      this.source = source;
      this.readInstance = readInstance;
    }

    @Override
    public List<? extends BoundedSource<T>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {

      List<BoundedDataSource<T>> splits = source.split(desiredBundleSizeBytes);
      return splits.stream()
          .map(source -> new BoundedSourceWrapper<>(source, readInstance))
          .collect(Collectors.toList());
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return source.sizeEstimate();
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      cz.seznam.euphoria.core.client.io.BoundedReader<T> boundedReader = source.openReader();
      return new BoundedReaderWrapper<>(boundedReader, this);
    }

    @Override
    public Coder<T> getOutputCoder() {
      return readInstance.getOutputCoder();
    }
  }

  public static class BoundedReaderWrapper<T> extends BoundedSource.BoundedReader<T> {
    private final BoundedReader<T> euphoriaReader;
    private final BoundedSourceWrapper<T> boundedSourceWrapper;
    private T currentElement;

    public BoundedReaderWrapper(
        BoundedReader<T> euphoriaReader, BoundedSourceWrapper<T> boundedSourceWrapper) {
      this.euphoriaReader = euphoriaReader;
      this.boundedSourceWrapper = boundedSourceWrapper;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      boolean hasNext = euphoriaReader.hasNext();
      if (hasNext) {
        currentElement = euphoriaReader.next();
      }
      return hasNext;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return currentElement;
    }

    @Override
    public void close() throws IOException {
      euphoriaReader.close();
    }

    @Override
    public BoundedSource<T> getCurrentSource() {
      return boundedSourceWrapper;
    }

  }
}
