package cz.seznam.euphoria.beam.io;

import static cz.seznam.euphoria.shadow.com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import java.io.IOException;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;

public class EuphoriaIO {

  public static <T> Write<T> write(DataSink<T> sink, int numPartitions) {
    return new Write<>(sink, numPartitions);
  }

  /**
   * Write transform for euphoria sinks.
   *
   * @param <T> type of data to write
   */
  public static class Write<T> extends PTransform<PCollection<T>, PDone> {

    private final DataSink<T> sink;
    private final int numPartitions;

    private Write(DataSink<T> sink, int numPartitions) {
      this.sink = sink;
      this.numPartitions = numPartitions;
    }
    @Override
    public PDone expand(PCollection<T> input) {
      checkArgument(
          input.isBounded() == PCollection.IsBounded.BOUNDED,
          "Write supports bounded PCollections only.");

      // right now this is probably the best place to initialize sink
      sink.initialize();

      input
          .apply("extract-partition", ParDo.of(new PartitionFn<>(numPartitions)))
          .setTypeDescriptor(
              TypeDescriptors.kvs(TypeDescriptors.integers(), input.getTypeDescriptor()))
          .apply("group-by-partition", GroupByKey.create())
          .apply("write-result", ParDo.of(new WriteFn<>(sink)))
          .apply(Combine.globally((it) ->
              (int) StreamSupport.stream(it.spliterator(), false).count()))
          .apply(ParDo.of(new DoFn<Integer, Void>() {

            @SuppressWarnings("unused")
            @ProcessElement
            public void processElement() throws IOException {
              sink.commit();
            }
          }));

      return PDone.in(input.getPipeline());
    }
  }

  private static class PartitionFn<T> extends DoFn<T, KV<Integer, T>> {

    private final int numPartitions;

    PartitionFn(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(
        @Element T element, OutputReceiver<KV<Integer, T>> outputReceiver) {
      final int partition = (element.hashCode() & Integer.MAX_VALUE) % numPartitions;
      outputReceiver.output(KV.of(partition, element));
    }
  }

  private static class WriteFn<T> extends DoFn<KV<Integer, Iterable<T>>, Integer> {

    private final DataSink<T> sink;

    WriteFn(DataSink<T> sink) {
      this.sink = sink;
    }

    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(@Element KV<Integer, Iterable<T>> element) throws IOException {
      final Writer<T> writer = sink.openWriter(requireNonNull(element.getKey()));
      try {
        for (T item : element.getValue()) {
          writer.write(item);
        }
        writer.flush();
        writer.commit();
      } catch (IOException e) {
        writer.rollback();
      } finally {
        writer.close();
      }
    }
  }
}
