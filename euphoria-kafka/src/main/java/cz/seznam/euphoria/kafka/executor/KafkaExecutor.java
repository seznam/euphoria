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

package cz.seznam.euphoria.kafka.executor;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.inmem.operator.StreamElement;
import cz.seznam.euphoria.shaded.guava.com.google.common.annotations.VisibleForTesting;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@code Executor} passing data between pipelines via Apache Kafka topics.
 */
public class KafkaExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaExecutor.class);

  // FIXME
  private final int globalId = 0;
  private final ExecutorService executor;
  private final String[] bootstrapServers;
  private final ExecutionContext context = new ExecutionContext();


  public KafkaExecutor(ExecutorService executor, String[] bootstrapServers) {
    this.executor = executor;
    this.bootstrapServers = bootstrapServers;
  }

  @Override
  public CompletableFuture<Result> submit(Flow flow) {
    return CompletableFuture.supplyAsync(() -> execute(flow), executor);
  }

  @Override
  public void shutdown() {
    executor.shutdownNow();
  }

  private Result execute(Flow flow) {
    DAG<Operator<?, ?>> unfolded = FlowUnfolder.unfold(flow, getBasicOps());
    int count = unfolded.size() + unfolded.getLeafs().size();
    CountDownLatch latch = new CountDownLatch(count);
    CountDownLatch finishLatch = new CountDownLatch(unfolded.getLeafs().size());
    LOG.debug("Will wait for {} operators to start up", count);

    // first run all operators
    unfolded.traverse().forEachOrdered(node -> runNode(node, latch));

    // next start to consume outputs
    unfolded.getLeafs().forEach(n -> observeOutput(n, latch, finishLatch));

    try {
      // wait until the computation ends
      for (;;) {
        if (finishLatch.await(10, TimeUnit.SECONDS)) {
          break;
        }
        LOG.info(
            "Waiting for the end of computation, still {} output partitions not finished",
            finishLatch.getCount());
      }
    } catch (InterruptedException ex) {
      LOG.warn("Interrupted waiting for the end of computation.");
    }

    LOG.info("Computation successfully completed");

    return new Result();
  }

  private Set<Class<? extends Operator<?, ?>>> getBasicOps() {
    // use the "standard" set of basic operators
    return Executor.getBasicOps();
  }

  @SuppressWarnings("unchecked")
  private void runNode(Node<Operator<?, ?>> node, CountDownLatch latch) {
    Operator<?, ?> op = node.get();

    if (op instanceof FlatMap) {
      flatMap(node, (FlatMap) op, latch);
    } else if (op instanceof Repartition) {
      repartition(node, (Repartition) op, latch);
    } else if (op instanceof Union) {
      union(node, (Union) op, latch);
    } else if (op instanceof ReduceStateByKey) {
      reduceStateByKey(node, (ReduceStateByKey) op, latch);
    } else if (op instanceof FlowUnfolder.InputOperator) {
      consumeSource(op.output(), latch);
    } else {
      throw new IllegalArgumentException("Invalid operator " + op + ". Fix code.");
    }
  }

  @SuppressWarnings("unchecked")
  private void flatMap(
      Node<Operator<?, ?>> node,
      FlatMap<?, ?> op,
      CountDownLatch latch) {

    Dataset<?> input = Iterables.getOnlyElement(op.listInputs());
    ObservableStream<KafkaStreamElement> stream = context.get(input);
    UnaryFunctor functor = op.getFunctor();
    BlockingQueue<KafkaStreamElement> output = new ArrayBlockingQueue<>(100);
    BlockingQueueObservableStream<KafkaStreamElement> outputStream;
    outputStream = BlockingQueueObservableStream.wrap(output, globalId);
    context.register(op.output(), outputStream);

    stream.observe(nameForConsumer(input), new StreamObserver<KafkaStreamElement>() {

      @Override
      public void onRegistered() {
        latch.countDown();
        awaitLatch(latch);
        LOG.info(
            "Started to observe input stream {} of operator {}",
            input, op.getName());
      }

      @Override
      public void onNext(int partitionId, KafkaStreamElement elem) {
        if (elem.isElement()) {
          Context<Object> context = mapContext(elem, output);
          functor.apply(elem.getElement(), context);
        } else {
          try {
            output.put(elem);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
      }

      @Override
      public void onError(Throwable err) {
        // FIXME
        LOG.error("Error on stream", err);
        onCompleted();
      }

      @Override
      public void onCompleted() {
        try {
          output.put(KafkaStreamElement.FACTORY.endOfStream());
        } catch (InterruptedException ex) {
          LOG.warn("Interrupted while finishing the calculation");
          Thread.currentThread().interrupt();
        }
      }

    });
  }

  private void repartition(
      Node<Operator<?, ?>> node,
      Repartition<?> op,
      CountDownLatch latch) {

    latch.countDown();
    awaitLatch(latch);
  }

  private void union(
      Node<Operator<?, ?>> node,
      Union<?> op,
      CountDownLatch latch) {

    latch.countDown();
    awaitLatch(latch);
  }

  private void reduceStateByKey(
      Node<Operator<?, ?>> node,
      ReduceStateByKey<?, ?, ?, ?, ?, ?, ?, ?, ?> op,
      CountDownLatch latch) {

    
    latch.countDown();
    awaitLatch(latch);
  }

  /**
   * Create observable stream from dataset.
   */
  @VisibleForTesting
  ObservableStream<?> kafkaObservableStream(
      Dataset<?> dataset,
      Function<ConsumerRecord<byte[], byte[]>, KafkaStreamElement> deserializer) {
    
    return new KafkaObservableStream<>(
        executor, bootstrapServers,
        nameForConsumer(dataset),
        deserializer);
  }

  private static String nameForConsumer(Dataset<?> dataset) {
    Operator<?, ?> producer = dataset.getProducer();
    DataSource<?> source = dataset.getSource();
    return "__euphoria_" + (producer == null
        ? source.toString()
        : producer.getName());
  }

  @SuppressWarnings("unchecked")
  private Context<Object> mapContext(
      StreamElement input,
      BlockingQueue<KafkaStreamElement> output) {

    return new Context<Object>() {
      @Override
      public void collect(Object elem) {
        try {
          output.put(KafkaStreamElement.FACTORY.data(
              elem, input.getWindow(), input.getTimestamp()));
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }

      @Override
      public Object getWindow() {
        return input.getWindow();
      }
    };
  }


  @SuppressWarnings("unchecked")
  void observeOutput(
      Node<Operator<?, ?>> outputOp,
      CountDownLatch startLatch,
      CountDownLatch finishLatch) {

    Dataset<?> dataset = outputOp.get().output();
    ObservableStream<KafkaStreamElement> output = context.get(dataset);
    DataSink<?> sink = Objects.requireNonNull(
        dataset.getOutputSink(),
        "Missing datasink of leaf dataset " + dataset
            + ", producer " + dataset.getProducer());

    sink.initialize();
    Writer<Object> writer = (Writer) sink.openWriter(globalId);
    
    output.observe(nameForConsumer(dataset), new StreamObserver<KafkaStreamElement>() {
      
      @Override
      public void onRegistered() {
        startLatch.countDown();
        awaitLatch(startLatch);
        LOG.info(
            "Started to observe dataset {} writing data to {}",
            dataset, sink);
      }

      @Override
      public void onNext(int partitionId, KafkaStreamElement elem) {
        if (elem.isElement()) {
          try {
            writer.write(elem.getElement());
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        } else if (elem.isEndOfStream()) {
          onCompleted();
        }
      }

      @Override
      public void onError(Throwable err) {
        try {
          finishLatch.countDown();
          writer.rollback();
          writer.close();
          LOG.error(
              "Error observing output stream of producer {}",
              dataset.getProducer());
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }

      @Override
      public void onCompleted() {
        try {
          finishLatch.countDown();
          writer.flush();
          writer.commit();
          writer.close();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }

    });
  }


  @SuppressWarnings("unchecked")
  private void consumeSource(Dataset<?> dataset, CountDownLatch latch) {
    
    BlockingQueue<KafkaStreamElement> output = new ArrayBlockingQueue<>(100);
    BlockingQueueObservableStream<KafkaStreamElement> outputStream;
    outputStream = BlockingQueueObservableStream.wrap(output, globalId);
    DataSource<?> source = Objects.requireNonNull(
        dataset.getSource(), "Root node in DAG always has to have source");
    context.register(dataset, outputStream);

    List<Partition<?>> partitions = (List<Partition<?>>) source.getPartitions();
    if (globalId == 0) {
      // FIXME: this is wrong, need a way to balance the inputs here
      CountDownLatch finishLatch = new CountDownLatch(partitions.size());
      partitions.forEach(p -> {

        executor.execute(() -> {
          latch.countDown();
          try {
            awaitLatch(latch);
            LOG.info("Started thread consuming source {}", source);
            Reader<?> reader = p.openReader();
            while (reader.hasNext()) {
              Object next = reader.next();
              output.put(
                  KafkaStreamElement.FACTORY.data(next,
                  Batch.BatchWindow.get(),
                  System.currentTimeMillis()));
            }
          } catch (IOException ex) {
            LOG.error("Failed reading input {}", p, ex);
            throw new RuntimeException(ex);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
          finishLatch.countDown();
        });
      });
      // thread that will wait until all the partitions end
      executor.execute(() -> {
        try {
          finishLatch.await();
          output.put(KafkaStreamElement.FACTORY.endOfStream());
        } catch (InterruptedException ex) {
          LOG.warn("Interrupted while waiting for stream ends.");
        }
      });
    }
  }

  private void awaitLatch(CountDownLatch latch) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new RuntimeException(
            "Timeout waiting for the operators to start up.");
      }
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

}
