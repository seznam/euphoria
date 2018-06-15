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
package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.ExceptionUtils;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import cz.seznam.euphoria.testing.DatasetAssert;
import java.nio.file.Paths;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests {@link HadoopSink} implementations. Each implementation is tested with and without {@link
 * org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat} to see whether it produces empty result
 * files (with {@link org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat} no empty files should
 * be created).
 *
 * @param <I> type of objects in data source (input)
 * @param <O> type of objects in output
 * @param <S> data source type
 * @param <T> data sink (output) type
 */
@RunWith(Parameterized.class)
public class HadoopSinkTest<I, O, S extends DataSource<I>, T extends DataSink<I>> {

  @Parameters(name = "{index}: {0}")
  public static List<Object[]> testParameters() {
    return Arrays.asList(
        new Object[][] {
          // testName, DataSinkTester, useLazyOutputFormat, expectedNumberOfReduceOutputs
          {"SequenceFileSink", new SequenceFileSinkTester(), false, 5},
          {"SequenceFileSink - lazy", new SequenceFileSinkTester(), true, 4},
          {"HadoopTextFileSink", new HadoopTextFileSinkTester(), false, 5},
          {"HadoopTextFileSink - lazy", new HadoopTextFileSinkTester(), true, 4},
          {"SimpleHadoopTextFileSink", new SimpleHadoopTextFileSinkTester(), false, 5},
          {"SimpleHadoopTextFileSink - lazy", new SimpleHadoopTextFileSinkTester(), true, 4},
          {"HadoopToStringSink", new HadoopToStringSinkTester(), false, 5},
          {"HadoopToStringSink - lazy", new HadoopToStringSinkTester(), true, 4}
        });
  }

  private final String testName;
  private final DataSinkTester<I, O, S, T> dataSinkTester;
  private final boolean useLazyOutputFormat;
  private final int expectedNumberOfReduceOutputs;

  public HadoopSinkTest(
      String testName,
      DataSinkTester<I, O, S, T> dataSinkTester,
      boolean useLazyOutputFormat,
      int expectedNumberOfReduceOutputs) {
    this.testName = testName;
    this.dataSinkTester = dataSinkTester;
    this.useLazyOutputFormat = useLazyOutputFormat;
    this.expectedNumberOfReduceOutputs = expectedNumberOfReduceOutputs;
  }

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void test() {
    final Configuration conf = new Configuration();

    final String outputDir =
        Paths.get(tmp.getRoot().getAbsolutePath(), testName).toAbsolutePath().toString();

    final Flow flow = Flow.create();

    final S source = dataSinkTester.prepareDataSource();
    final T sink = dataSinkTester.buildSink(outputDir, conf, useLazyOutputFormat);

    MapElements.of(flow.createInput(source)).using(p -> p).output().persist(sink);

    final Executor executor = new LocalExecutor().setDefaultParallelism(4);
    executor.submit(flow).join();

    String[] files = new File(outputDir).list();
    assertNotNull(files);

    List<String> reduceOutputFileNames =
        Arrays.stream(files)
            .filter(file -> file.startsWith("part-r-"))
            .collect(Collectors.toList());
    assertEquals(expectedNumberOfReduceOutputs, reduceOutputFileNames.size());

    final List<O> output =
        reduceOutputFileNames
            .stream()
            .flatMap(dataSinkTester.extractOutputFunction(outputDir, conf))
            .collect(Collectors.toList());

    DatasetAssert.unorderedEquals(dataSinkTester.expectedOutput(), output);
  }

  /**
   * Helper interface for parameterized tests.
   *
   * @param <I> type of objects in data source (input)
   * @param <O> type of objects in output
   * @param <S> data source type
   * @param <T> data sink (output) type
   */
  private interface DataSinkTester<I, O, S extends DataSource<I>, T extends DataSink<I>> {

    /** builds/creates desired output sink */
    T buildSink(String outputDir, Configuration conf, boolean useLazyOutputFormat);

    /** builds/creates desired input source (data source) */
    S prepareDataSource();

    /**
     * returns function which loads created file, reads its content a creates desired output stream
     * out of the content
     */
    Function<String, Stream<O>> extractOutputFunction(String outputDir, Configuration conf);

    /**
     * returns expected output which is then compared with result from {@link
     * #extractOutputFunction(String, Configuration)}
     */
    List<O> expectedOutput();
  }

  private static class SequenceFileSinkTester
      implements DataSinkTester<
          Pair<Text, LongWritable>,
          Pair<Text, LongWritable>,
          DataSource<Pair<Text, LongWritable>>,
          SequenceFileSink<Text, LongWritable>> {

    @SuppressWarnings("unchecked")
    @Override
    public SequenceFileSink<Text, LongWritable> buildSink(
        String outputDir, Configuration conf, boolean useLazyOutputFormat) {
      SequenceFileSink.OptionalBuilder<Text, LongWritable> builder =
          SequenceFileSink.of(Text.class, LongWritable.class)
              .outputPath(outputDir)
              .withConfiguration(conf)
              .withCompression(DeflateCodec.class, SequenceFile.CompressionType.BLOCK);
      if (useLazyOutputFormat) {
        builder = builder.withLazyOutputFormat();
      }
      return builder.build();
    }

    @Override
    public DataSource<Pair<Text, LongWritable>> prepareDataSource() {
      return ListDataSource.bounded(
          Collections.singletonList(Pair.of(new Text("first"), new LongWritable(1L))),
          Collections.singletonList(Pair.of(new Text("second"), new LongWritable(2L))),
          Collections.singletonList(Pair.of(new Text("third"), new LongWritable(3L))),
          Collections.singletonList(Pair.of(new Text("fourth"), new LongWritable(3L))),
          Collections.emptyList());
    }

    @Override
    public Function<String, Stream<Pair<Text, LongWritable>>> extractOutputFunction(
        String outputDir, Configuration conf) {
      return part ->
          ExceptionUtils.unchecked(
              () -> {
                try (final SequenceFileRecordReader<Text, LongWritable> reader =
                    new SequenceFileRecordReader<>()) {
                  final Path path = new Path(outputDir + "/" + part);
                  final TaskAttemptContext taskContext =
                      HadoopUtils.createTaskContext(new Configuration(), HadoopUtils.getJobID(), 0);
                  reader.initialize(
                      new FileSplit(path, 0L, Long.MAX_VALUE, new String[] {"localhost"}),
                      taskContext);
                  final List<Pair<Text, LongWritable>> result = new ArrayList<>();
                  while (reader.nextKeyValue()) {
                    result.add(Pair.of(reader.getCurrentKey(), reader.getCurrentValue()));
                  }
                  return result.stream();
                }
              });
    }

    @Override
    public List<Pair<Text, LongWritable>> expectedOutput() {
      return Arrays.asList(
          Pair.of(new Text("first"), new LongWritable(1L)),
          Pair.of(new Text("second"), new LongWritable(2L)),
          Pair.of(new Text("third"), new LongWritable(3L)),
          Pair.of(new Text("fourth"), new LongWritable(3L)));
    }
  }

  private static class HadoopTextFileSinkTester
      implements DataSinkTester<
          Pair<Text, LongWritable>,
          Pair<String, Long>,
          DataSource<Pair<Text, LongWritable>>,
          HadoopTextFileSink<Text, LongWritable>> {

    @SuppressWarnings("unchecked")
    @Override
    public HadoopTextFileSink<Text, LongWritable> buildSink(
        String outputDir, Configuration conf, boolean useLazyOutputFormat) {
      return new HadoopTextFileSink<>(outputDir, conf, useLazyOutputFormat);
    }

    @Override
    public DataSource<Pair<Text, LongWritable>> prepareDataSource() {
      return ListDataSource.bounded(
          Collections.singletonList(Pair.of(new Text("first"), new LongWritable(1L))),
          Collections.singletonList(Pair.of(new Text("second"), new LongWritable(2L))),
          Collections.singletonList(Pair.of(new Text("third"), new LongWritable(3L))),
          Collections.singletonList(Pair.of(new Text("fourth"), new LongWritable(3L))),
          Collections.emptyList());
    }

    @Override
    public Function<String, Stream<Pair<String, Long>>> extractOutputFunction(
        String outputDir, Configuration conf) {
      return part ->
          ExceptionUtils.unchecked(
              () -> {
                try (final KeyValueLineRecordReader reader = new KeyValueLineRecordReader(conf)) {
                  final Path path = new Path(outputDir + "/" + part);
                  final TaskAttemptContext taskContext =
                      HadoopUtils.createTaskContext(new Configuration(), HadoopUtils.getJobID(), 0);
                  reader.initialize(
                      new FileSplit(path, 0L, Long.MAX_VALUE, new String[] {"localhost"}),
                      taskContext);
                  final List<Pair<String, Long>> result = new ArrayList<>();
                  while (reader.nextKeyValue()) {
                    result.add(
                        Pair.of(
                            reader.getCurrentKey().toString(),
                            Long.valueOf(reader.getCurrentValue().toString())));
                  }
                  return result.stream();
                }
              });
    }

    @Override
    public List<Pair<String, Long>> expectedOutput() {
      return Arrays.asList(
          Pair.of("first", 1L), Pair.of("second", 2L), Pair.of("third", 3L), Pair.of("fourth", 3L));
    }
  }

  private static class SimpleHadoopTextFileSinkTester
      implements DataSinkTester<Text, String, DataSource<Text>, SimpleHadoopTextFileSink<Text>> {

    @Override
    public SimpleHadoopTextFileSink<Text> buildSink(
        String outputDir, Configuration conf, boolean useLazyOutputFormat) {
      return new SimpleHadoopTextFileSink<>(outputDir, conf, useLazyOutputFormat);
    }

    @Override
    public DataSource<Text> prepareDataSource() {
      return ListDataSource.bounded(
          Collections.singletonList(new Text("first")),
          Collections.singletonList(new Text("second")),
          Collections.singletonList(new Text("third")),
          Collections.singletonList(new Text("fourth")),
          Collections.emptyList());
    }

    @Override
    public Function<String, Stream<String>> extractOutputFunction(
        String outputDir, Configuration conf) {
      return part ->
          ExceptionUtils.unchecked(
              () -> {
                try (final LineRecordReader reader = new LineRecordReader()) {
                  final Path path = new Path(outputDir + "/" + part);
                  final TaskAttemptContext taskContext =
                      HadoopUtils.createTaskContext(new Configuration(), HadoopUtils.getJobID(), 0);
                  reader.initialize(
                      new FileSplit(path, 0L, Long.MAX_VALUE, new String[] {"localhost"}),
                      taskContext);
                  final List<String> result = new ArrayList<>();
                  while (reader.nextKeyValue()) {
                    result.add(reader.getCurrentValue().toString());
                  }
                  return result.stream();
                }
              });
    }

    @Override
    public List<String> expectedOutput() {
      return Arrays.asList("first", "second", "third", "fourth");
    }
  }

  private static class HadoopToStringSinkTester
      implements DataSinkTester<
          Pair<String, String>,
          String,
          DataSource<Pair<String, String>>,
          HadoopToStringSink<Pair<String, String>>> {

    @Override
    public HadoopToStringSink<Pair<String, String>> buildSink(
        String outputDir, Configuration conf, boolean useLazyOutputFormat) {
      return new HadoopToStringSink<>(outputDir, conf, useLazyOutputFormat);
    }

    @Override
    public DataSource<Pair<String, String>> prepareDataSource() {
      return ListDataSource.bounded(
          Collections.singletonList(Pair.of("first", "1")),
          Collections.singletonList(Pair.of("second", "2")),
          Collections.singletonList(Pair.of("third", "3")),
          Collections.singletonList(Pair.of("fourth", "4")),
          Collections.emptyList());
    }

    @Override
    public Function<String, Stream<String>> extractOutputFunction(
        String outputDir, Configuration conf) {
      return part ->
          ExceptionUtils.unchecked(
              () -> {
                try (final LineRecordReader reader = new LineRecordReader()) {
                  final Path path = new Path(outputDir + "/" + part);
                  final TaskAttemptContext taskContext =
                      HadoopUtils.createTaskContext(new Configuration(), HadoopUtils.getJobID(), 0);
                  reader.initialize(
                      new FileSplit(path, 0L, Long.MAX_VALUE, new String[] {"localhost"}),
                      taskContext);
                  final List<String> result = new ArrayList<>();
                  while (reader.nextKeyValue()) {
                    result.add(reader.getCurrentValue().toString());
                  }
                  return result.stream();
                }
              });
    }

    @Override
    public List<String> expectedOutput() {
      return Arrays.asList(
          Pair.of("first", "1").toString(),
          Pair.of("second", "2").toString(),
          Pair.of("third", "3").toString(),
          Pair.of("fourth", "4").toString());
    }
  }
}
