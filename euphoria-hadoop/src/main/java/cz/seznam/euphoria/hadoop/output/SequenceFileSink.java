/*
 * Copyright 2016-2019 Seznam.cz, a.s.
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

import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * A convenience data sink to produce output through hadoop's sequence file for a specified key and
 * value type.
 *
 * <p>Example:
 *
 * <pre>{@code
 * SequenceFileSink
 *    .of(KeyClass, ValueClass)
 *    .outputPath(outputDir)
 *    .withLazyOutputFormat() // optional
 *    .withConfiguration( hadoopConfig) // optional
 *    .withCompression( CompressionClass, CompressionType) //optional
 *    .build();
 * }</pre>
 *
 * @param <K> the type of the keys emitted
 * @param <V> the type of the values emitted
 */
public class SequenceFileSink<K, V> extends HadoopSink<K, V> {

  /**
   * @param keyClass the class representing the type of the keys emitted
   * @param valueClass the class representing the type of the values emitted
   * @param <K> type of the keys emitted
   * @param <V> type of the values emitted
   * @return OutputPathBuilder
   */
  public static <K, V> OutputPathBuilder<K, V> of(Class<K> keyClass, Class<V> valueClass) {
    return new Builder<>(keyClass, valueClass);
  }

  public static class Builder<K, V> implements OutputPathBuilder<K, V>, OptionalBuilder<K, V> {

    private final Class<K> keyClass;
    private final Class<V> valueClass;

    private String outputPath = null;
    private boolean useLazyOutputFormat = false;
    private Configuration configuration = null;
    private Class<? extends CompressionCodec> compressionClass = null;
    private SequenceFile.CompressionType compressionType = null;

    public Builder(Class<K> keyClass, Class<V> valueClass) {
      this.keyClass = Objects.requireNonNull(keyClass);
      this.valueClass = Objects.requireNonNull(valueClass);
    }

    @Override
    public OptionalBuilder<K, V> outputPath(String outputPath) {
      this.outputPath = Objects.requireNonNull(outputPath);
      return this;
    }

    @Override
    public OptionalBuilder<K, V> withConfiguration(Configuration configuration) {
      this.configuration = Objects.requireNonNull(configuration);
      return this;
    }

    @Override
    public OptionalBuilder<K, V> withCompression(
        Class<? extends CompressionCodec> compressionClass,
        SequenceFile.CompressionType compressionType) {
      this.compressionClass = Objects.requireNonNull(compressionClass);
      this.compressionType = Objects.requireNonNull(compressionType);
      return this;
    }

    @Override
    public OptionalBuilder<K, V> withLazyOutputFormat() {
      this.useLazyOutputFormat = true;
      return this;
    }

    @Override
    public SequenceFileSink<K, V> build() {
      final Configuration newConfiguration = wrap(configuration);

      if (compressionClass != null && compressionType != null) {
        newConfiguration.setBoolean(
            FileOutputFormat.COMPRESS, true);
        newConfiguration.set(
            FileOutputFormat.COMPRESS_TYPE, compressionType.toString());
        newConfiguration.setClass(
            FileOutputFormat.COMPRESS_CODEC, compressionClass, CompressionCodec.class);
      }

      return new SequenceFileSink<>(
          keyClass, valueClass, outputPath, newConfiguration, useLazyOutputFormat);
    }
  }

  public interface OutputPathBuilder<K, V> {

    /**
     * Mandatory output path.
     *
     * @param outputPath the destination where to save the output
     * @return Builder with optional setters
     */
    OptionalBuilder<K, V> outputPath(String outputPath);
  }

  public interface OptionalBuilder<K, V> {

    /**
     * Optional setter if not used it will be created new hadoop configuration.
     *
     * @param configuration the hadoop configuration to build on top of
     * @return Builder
     */
    OptionalBuilder<K, V> withConfiguration(Configuration configuration);

    /**
     * Optional setter for compression
     *
     * @param compressionClass COMPRESS_CODEC class
     * @param compressionType COMPRESS_TYPE value
     * @return Builder
     */
    OptionalBuilder<K, V> withCompression(
        Class<? extends CompressionCodec> compressionClass,
        SequenceFile.CompressionType compressionType);

    /**
     * Optional setter for {@link org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat}. If used,
     * {@link SequenceFileOutputFormat} won't create empty files. Default value is false;
     *
     * @return this instance of SinkBuilder
     */
    OptionalBuilder<K, V> withLazyOutputFormat();

    SequenceFileSink<K, V> build();
  }

  /**
   * Convenience constructor delegating to {@link #SequenceFileSink(Class, Class, String,
   * Configuration)} with a newly created hadoop configuration.
   *
   * @param keyType the type of the key consumed and emitted to the output by the new data sink
   * @param valueType the type of the value consumed and emitted to the output by the new data sink
   * @param path the destination where to place the output to
   * @deprecated will be in next release private, use {@link #of(Class, Class)} instead
   */
  public SequenceFileSink(Class<K> keyType, Class<V> valueType, String path) {
    this(keyType, valueType, path, null);
  }

  /**
   * Constructs a data sink based on hadoop's {@link SequenceFileOutputFormat}. The specified path
   * is automatically set/overridden in the given hadoop configuration as well as the key and value
   * types.
   *
   * @param keyType the class representing the type of the keys emitted
   * @param valueType the class representing the type of the values emitted
   * @param path the destination where to save the output
   * @param hadoopConfig the hadoop configuration to build on top of
   * @throws NullPointerException if any of the parameters is {@code null}
   * @deprecated will be in next release private, use {@link #of(Class, Class)} instead
   */
  @SuppressWarnings("unchecked")
  public SequenceFileSink(
      Class<K> keyType, Class<V> valueType, String path, Configuration hadoopConfig) {
    this(keyType, valueType, path, wrap(hadoopConfig), false);
  }

  /**
   * Constructs a data sink based on hadoop's {@link SequenceFileOutputFormat}. The specified path
   * is automatically set/overridden in the given hadoop configuration as well as the key and value
   * types.
   *
   * @param keyType the class representing the type of the keys emitted
   * @param valueType the class representing the type of the values emitted
   * @param path the destination where to save the output
   * @param hadoopConfig the hadoop configuration to build on top of
   * @param useLazyOutputFormat whether to use {@link
   *     org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat} (won't create empty files)
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  @SuppressWarnings("unchecked")
  private SequenceFileSink(
      Class<K> keyType,
      Class<V> valueType,
      String path,
      Configuration hadoopConfig,
      boolean useLazyOutputFormat) {
    super(
        (Class) SequenceFileOutputFormat.class,
        setCommonConfValues(hadoopConfig, path, keyType.getName(), valueType.getName()),
        useLazyOutputFormat);
  }

  private static Configuration wrap(Configuration conf) {
    return conf == null ? new Configuration() : new Configuration(conf);
  }

  private static Configuration setCommonConfValues(
      final Configuration conf, String path, String keyTypeName, String valueTypeName) {
    Objects.requireNonNull(conf);
    conf.set(FileOutputFormat.OUTDIR, path);
    conf.set(JobContext.OUTPUT_KEY_CLASS, keyTypeName);
    conf.set(JobContext.OUTPUT_VALUE_CLASS, valueTypeName);
    return conf;
  }
}
