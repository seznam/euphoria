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
package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A general purpose data source based on top of hadoop input formats.
 *
 * @param <K> the type of record keys
 * @param <V> the type of record values
 */
public class HadoopFileSource<K, V> extends HadoopSource<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopFileSource.class);

  /**
   * Split size to use, if we are not satisfied with the one user provided.
   */
  private static final long MIN_SPLIT_SIZE = 128 * 1024 * 1024;

  @SuppressWarnings("unchecked")
  public HadoopFileSource(
      Class<K> keyClass,
      Class<V> valueClass,
      Class<? extends FileInputFormat> inputFormatClass,
      Configuration conf) {
    super(keyClass, valueClass, (Class) inputFormatClass, conf);
  }

  @Override
  public List<BoundedDataSource<KV<K, V>>> split(long desiredSplitSizeBytes) {
    final Job job = newJob();
    final long configuredSplitSize = getConf().getLong(FileInputFormat.SPLIT_MAXSIZE, MIN_SPLIT_SIZE);
    long splitSize = Math.max(configuredSplitSize, desiredSplitSizeBytes);
    LOG.info(String.format("desiredSplitSizeBytes  %,d,  configuredSplitSize  %,d.",
        desiredSplitSizeBytes, configuredSplitSize));
    LOG.info(String.format("%s's max and min input split size will be set to %,d .",
        FileInputFormat.class.getSimpleName(), splitSize));

    FileInputFormat.setMinInputSplitSize(job, splitSize);
    FileInputFormat.setMaxInputSplitSize(job, splitSize);
    return doSplit(job);
  }

}
