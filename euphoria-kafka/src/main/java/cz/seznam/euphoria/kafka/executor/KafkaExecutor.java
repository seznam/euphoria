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

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioner;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.operator.ExtractEventTime;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StateMerger;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.inmem.InMemStorageProvider;
import cz.seznam.euphoria.inmem.NoopTriggerScheduler;
import cz.seznam.euphoria.inmem.VectorClock;
import cz.seznam.euphoria.inmem.WatermarkEmitStrategy;
import cz.seznam.euphoria.inmem.WatermarkTriggerScheduler;
import cz.seznam.euphoria.inmem.operator.Collector;
import cz.seznam.euphoria.inmem.operator.ReduceStateByKeyReducer;
import cz.seznam.euphoria.inmem.operator.StreamElement;
import cz.seznam.euphoria.shaded.guava.com.google.common.annotations.VisibleForTesting;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@code Executor} passing data between pipelines via Apache Kafka topics.
 */
public class KafkaExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaExecutor.class);

  // FIXME: this has to be shared across a cluster and from interval (0, N - 1)
  // use akka to distribute this acroos multiple nodes
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
    unfolded.traverse().forEachOrdered(node -> runNode(flow, node, latch));

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
  private void runNode(
      Flow flow,
      Node<Operator<?, ?>> node,
      CountDownLatch latch) {

    Operator<?, ?> op = node.get();

    LOG.debug("Executing DAG node {}", node);
    if (op instanceof FlatMap) {
      flatMap(node, (FlatMap) op, latch);
    } else if (op instanceof Repartition) {
      repartition(flow, node, (Repartition) op, latch);
    } else if (op instanceof Union) {
      union(node, (Union) op, latch);
    } else if (op instanceof ReduceStateByKey) {
      reduceStateByKey(flow, node, (ReduceStateByKey) op, latch);
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

    final Dataset<?> input = Iterables.getOnlyElement(node.getParents()).get().output();
    final ObservableStream<KafkaStreamElement> stream = context.get(input);
    final UnaryFunctor functor = op.getFunctor();
    final List<Pair<BlockingQueue<KafkaStreamElement>, Integer>> outputs;
    final BlockingQueueObservableStream<KafkaStreamElement> outputStream;

    outputs = createOutputQueues(stream.size());    
    outputStream = BlockingQueueObservableStream.wrap(
        executor, op.getName(), outputs);
    context.register(op.output(), outputStream);

    stream.observe(nameForConsumerOf(input), new StreamObserver<KafkaStreamElement>() {

      @Override
      public void onRegistered() {
        registerOperator(op, latch);
      }

      @Override
      public void onNext(int partitionId, KafkaStreamElement elem) {
        BlockingQueue<KafkaStreamElement> output = outputs.get(partitionId).getFirst();
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
        LOG.error("Error on stream of operator {}", op.getName(), err);
        onCompleted();
      }

      @Override
      public void onCompleted() {
        outputs.forEach(output -> closeStream(output.getFirst()));
        LOG.info("Finished processing of operator {}", op.getName());
      }

    });
  }

  @VisibleForTesting
  protected List<Pair<BlockingQueue<KafkaStreamElement>, Integer>> createOutputQueues(
      int count) {
    List<Pair<BlockingQueue<KafkaStreamElement>, Integer>> ret = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ret.add(Pair.of(new ArrayBlockingQueue<>(100), i));
    }
    return ret;
  }

  private void repartition(
      Flow flow,
      Node<Operator<?, ?>> node,
      Repartition<?> op,
      CountDownLatch latch) {

    Dataset<?> input = Iterables.getOnlyElement(node.getParents()).get().output();
    ObservableStream<KafkaStreamElement> stream = context.get(input);
    Partitioning<?> partitioning = op.getPartitioning();

    String topic = nameForConsumerOf(op.output());
    Partitioner partitioner = partitioning.getPartitioner();
    int numPartitions = numPartitions(
        input.getNumPartitions(),
        partitioning.getNumPartitions());

    final List<Pair<BlockingQueue<KafkaStreamElement>, Integer>> outputQueues;
    final BlockingQueueObservableStream<KafkaStreamElement> outputStream;
    outputQueues = createOutputQueues(numPartitions);
    outputStream = BlockingQueueObservableStream.wrap(
        executor, op.getName(), outputQueues);

    String topicName = "__euphoria_" + flowName(flow) + "_" + op.getName();
    OutputWriter writer = outputWriter(topicName, numPartitions);
    context.register(op.output(), outputStream);

    ObservableStream<KafkaStreamElement> repartitioned;
    repartitioned = kafkaObservableStream(topicName, this::fromBytes);

    CountDownLatch runningParts = new CountDownLatch(2);

    stream.observe(
        nameForConsumerOf(input) + "_send",
        sendingObserver(
            op, runningParts, latch, e -> e, partitioner, numPartitions, writer));


    VectorClock clock = new VectorClock(input.getNumPartitions());

    repartitioned.observe(
        nameForConsumerOf(input) + "_receive",
        receivingObserver(
            op, runningParts, latch, outputQueues, clock,
            Optional.empty()));

    awaitLatch(runningParts);
    LOG.info("Started both ends of repartition operator {}", op.getName());
    latch.countDown();

  }

  private String flowName(Flow flow) {
    if (flow.getName() != null) {
      return flow.getName();
    }
    // FIXME
    return "unnamed-flow";
  }

  @SuppressWarnings("unchecked")
  private StreamObserver<KafkaStreamElement> receivingObserver(
      Operator op,
      CountDownLatch runningParts,
      CountDownLatch latch,
      List<Pair<BlockingQueue<KafkaStreamElement>, Integer>> outputQueues,
      VectorClock clock,
      Optional<ExtractEventTime> eventTimeAssigner) {

    return new StreamObserver<KafkaStreamElement>() {

      @Override
      public void onRegistered() {
        runningParts.countDown();
        LOG.info("Started receiving part of operator {}", op.getName());
      }

      @Override
      public void onNext(int partitionId, KafkaStreamElement elem) {
        
        try {
          if (elem.isElement() || elem.isWindowTrigger()) {
            BlockingQueue<KafkaStreamElement> output;
            output = outputQueues.get(partitionId).getFirst();
            if (elem.isElement() && eventTimeAssigner.isPresent()) {
              elem.reassignTimestamp(
                  eventTimeAssigner.get().extractTimestamp(elem.getElement()));
            }
            output.put(elem);
          } else if (elem.isWatermark()) {
            long now = clock.getCurrent();
            clock.update(elem.getTimestamp(), partitionId);
            long updated = clock.getCurrent();
            if (updated != now) {
              outputQueues.forEach(output -> {
                try {
                  output.getFirst().put(KafkaStreamElement.FACTORY.watermark(updated));
                } catch (InterruptedException ex) {
                  Thread.currentThread().interrupt();
                }
              });
            }
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }

      @Override
      public void onError(Throwable err) {
        // FIXME: error handling
        LOG.error("Error on receiving part of operator {}", op.getName(), err);
        onCompleted();
      }

      @Override
      public void onCompleted() {
        outputQueues.forEach(output -> {
          try {
            output.getFirst().put(KafkaStreamElement.FACTORY.endOfStream());
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        });
      }

    };
  }

  @SuppressWarnings("unchecked")
  private StreamObserver<KafkaStreamElement> sendingObserver(
      Operator op,
      CountDownLatch runningParts,
      CountDownLatch latch,
      UnaryFunction keyExtractor,
      Partitioner partitioner,
      int numPartitions,
      OutputWriter writer) {

    return new StreamObserver<KafkaStreamElement>() {

      @Override
      public void onRegistered() {
        runningParts.countDown();
        LOG.info("Started sending part of operator {}", op.getName());
      }

      @Override
      public void onNext(int source, KafkaStreamElement elem) {
        if (elem.isElement()) {
          int target = partitioner.getPartition(
              keyExtractor.apply(elem.getElement())) & Integer.MAX_VALUE
              % numPartitions;
          writer.write(elem, source, target, (succ, exc) -> {
            // FIXME: error handling
            if (!succ) {
              throw new RuntimeException(exc);
            }
          });
        } else {
          // send to all downstream partitions
          for (int i = 0; i < numPartitions; i++) {
            writer.write(elem, source, i, (succ, exc) -> {
              // FIXME: error handling
              if (!succ) {
                throw new RuntimeException(exc);
              }
            });
          }
        }
      }

      @Override
      public void onError(Throwable err) {
        // FIXME: error handling
        LOG.error("Error on stream of operator {}", op.getName(), err);
        onCompleted();
      }

      @Override
      public void onCompleted() {
        for (int i = 0; i < numPartitions; i++) {
          writer.write(
              KafkaStreamElement.FACTORY.endOfStream(),
              0 /* FIXME */, i, (succ, exc) -> {
                // FIXME: error handling
                if (!succ) {
                  throw new RuntimeException(exc);
                }
              });
        }
      }
      
    };
  }

  @SuppressWarnings("unchecked")
  private void union(
      Node<Operator<?, ?>> node,
      Union<?> op,
      CountDownLatch latch) {

    List<Dataset<?>> inputs = node.getParents()
        .stream().map(n -> n.get().output())
        .collect(Collectors.toList());
    List<Pair<String, ObservableStream<KafkaStreamElement>>> streams = inputs.stream()
        .map(dataset -> Pair.of(nameForConsumerOf(dataset), context.get(dataset)))
        .collect(Collectors.toList());
    final List<Pair<BlockingQueue<KafkaStreamElement>, Integer>> outputs;
    final BlockingQueueObservableStream<KafkaStreamElement> outputStream;

    outputs = createOutputQueues(op.output().getNumPartitions());
    outputStream = BlockingQueueObservableStream.wrap(
        executor, op.getName(), outputs);
    context.register(op.output(), outputStream);
    CountDownLatch partitionsLatch = new CountDownLatch(streams.size());
    VectorClock clock = new VectorClock(streams.size());
    AtomicInteger finished = new AtomicInteger(0);

    streams.forEach(p -> {
      p.getSecond().observe(p.getFirst(), new StreamObserver<KafkaStreamElement>() {

        @Override
        public void onRegistered() {
          partitionsLatch.countDown();
          LOG.info("Started operator {}", op.getName());
        }
        
        @Override
        public void onNext(int partitionId, KafkaStreamElement elem) {
          BlockingQueue<KafkaStreamElement> output = outputs.get(partitionId).getFirst();
          try {
            if (elem.isElement() || elem.isWindowTrigger()) {
              // just blindly forward to output
              output.put(elem);
            } else if (elem.isWatermark()) {
              long now = clock.getCurrent();
              clock.update(elem.getTimestamp(), partitionId);
              long updated = clock.getCurrent();
              if (updated != now) {
                output.put(KafkaStreamElement.FACTORY.watermark(updated));
              }
            }
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }

        @Override
        public void onError(Throwable err) {
          // FIXME: error handling
          LOG.error("Error on stream of operator {}", op.getName(), err);
          onCompleted();
        }

        @Override
        public void onCompleted() {
          int total = finished.incrementAndGet();
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Finished processing of union of stream {} of totally finished {} of {}",
                new String[] {
                  String.valueOf(p.getSecond()),
                  String.valueOf(total),
                  String.valueOf(streams.size())
                });
          }

          if (total == streams.size()) {
            outputs.forEach(output -> {
              try {
                output.getFirst().put(KafkaStreamElement.FACTORY.endOfStream());
              } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
              }
            });
          }
        }
      });
    });

    awaitLatch(partitionsLatch);
    latch.countDown();

  }

  @SuppressWarnings("unchecked")
  private void reduceStateByKey(
      Flow flow,
      Node<Operator<?, ?>> node,
      ReduceStateByKey<?, ?, ?, ?, ?, ?, ?, ?, ?> op,
      CountDownLatch latch) {


    Dataset<?> input = Iterables.getOnlyElement(node.getParents()).get().output();
    ObservableStream<KafkaStreamElement> stream = context.get(input);
    StateFactory stateFactory = op.getStateFactory();
    StateMerger stateMerger = op.getStateMerger();
    UnaryFunction keyExtractor = op.getKeyExtractor();
    UnaryFunction valueExtractor = op.getValueExtractor();
    Windowing windowing = op.getWindowing();
    Partitioning partitioning = op.getPartitioning();
    Partitioner partitioner = partitioning.getPartitioner();
    ExtractEventTime eventTimeAssigner = op.getEventTimeAssigner();
    int numPartitions = numPartitions(
        input.getNumPartitions(),
        partitioning.getNumPartitions());
    final List<Pair<BlockingQueue<KafkaStreamElement>, Integer>> outputs;
    final List<Pair<BlockingQueue<KafkaStreamElement>, Integer>> repartitions;

    BlockingQueueObservableStream<KafkaStreamElement> repartitionStream;
    BlockingQueueObservableStream<KafkaStreamElement> outputStream;
    String topicName = "__euphoria_" + flowName(flow) + "_" + op.getName();
    OutputWriter repartitionWriter = outputWriter(topicName, numPartitions);
    ObservableStream<KafkaStreamElement> repartitioned;
    VectorClock clock = new VectorClock(input.getNumPartitions());

    outputs = createOutputQueues(numPartitions);
    repartitions = createOutputQueues(numPartitions);
    
    repartitioned = kafkaObservableStream(topicName, this::fromBytes);
    repartitionStream = BlockingQueueObservableStream.wrap(
        executor, op.getName(), repartitions);
    outputStream = BlockingQueueObservableStream.wrap(
        executor, op.getName(), outputs);
    context.register(op.output(), outputStream);

    CountDownLatch parts = new CountDownLatch(3);

    stream.observe(
        nameForConsumerOf(input) + "_send",
        sendingObserver(op, parts, latch, keyExtractor, partitioner,
            numPartitions, repartitionWriter));

    repartitioned.observe(
        nameForConsumerOf(input) + "_process",
        receivingObserver(
            op, parts, latch, repartitions, clock,
            Optional.ofNullable(eventTimeAssigner)));


    List<ReduceStateByKeyReducer> reducers = new ArrayList<>();
    for (int p = 0; p < numPartitions; p++) {
      BlockingQueue<KafkaStreamElement> output = outputs.get(p).getFirst();
      ReduceStateByKeyReducer reducer = new ReduceStateByKeyReducer(
          op, op.getName(),
          collector(output),
          keyExtractor,
          valueExtractor,
          // FIXME
          op.input().isBounded()
              ? new NoopTriggerScheduler()
              : (windowing != null
                  ? new WatermarkTriggerScheduler(500)
                  : new WatermarkTriggerScheduler(500)),
          new WatermarkEmitStrategy.Default(),
          new InMemStorageProvider(),
          KafkaStreamElement.FACTORY);

      reducer.setup();
      reducers.add(reducer);
    }

    repartitionStream.observe(
        nameForConsumerOf(input) + "_reduce",
        reduceStream(op, parts, latch, reducers));

    awaitLatch(parts);
    LOG.info("Started all parts of RSBK {}", op.getName());
    latch.countDown();
  }

  private StreamObserver<KafkaStreamElement> reduceStream(
      ReduceStateByKey op,
      CountDownLatch parts,
      CountDownLatch latch,
      List<ReduceStateByKeyReducer> reducers) {

    return new StreamObserver<KafkaStreamElement>() {
      @Override
      public void onRegistered() {
        parts.countDown();
        LOG.info("Started reducing part of operator {}", op.getName());
      }
      @Override
      public void onNext(int partitionId, KafkaStreamElement elem) {
        // ensure we will write the result into the correct output partition
        reducers.get(partitionId).accept(elem);
      }

      @Override
      public void onError(Throwable err) {
        LOG.error("Error during reduce of RSBK {}", op.getName(), err);
        onCompleted();
      }

      @Override
      public void onCompleted() {
        KafkaStreamElement eos = KafkaStreamElement.FACTORY.endOfStream();
        reducers.forEach(reducer -> reducer.accept(eos));
      }
    };
  }


  private <T extends StreamElement<?>> Collector<T> collector(BlockingQueue<T> queue) {

    return elem -> {
      try {
        queue.put(elem);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    };
  }

  /**
   * Create observable stream from dataset.
   */
  @VisibleForTesting
  ObservableStream<KafkaStreamElement> kafkaObservableStream(
      String topic,
      Function<byte[], Object> deserializer) {
    
    return new KafkaObservableStream(
        executor, bootstrapServers,
        topic, deserializer);
  }

  /**
   * Create writer for given dataset.
   */
  @SuppressWarnings("unchecked")
  @VisibleForTesting
  OutputWriter outputWriter(String topic, int parallelism) {
    KafkaProducer<byte[], byte[]> producer = createProducer();

    return (elem, source, target, callback) -> {
      // FIXME: serialization
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
          topic, target, elem.getTimestamp(), String.valueOf(source).getBytes(),
          toBytes(elem));
      producer.send(record, (meta, exc) -> callback.apply(exc == null, exc));
    };
  }

  @VisibleForTesting
  static String nameForConsumerOf(Dataset<?> dataset) {
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
    List<Writer<Object>> writers = new ArrayList<>(dataset.getNumPartitions());
    for (int p = 0; p < dataset.getNumPartitions(); p++) {
      writers.add((Writer) sink.openWriter(p));
    }
    
    output.observe(nameForConsumerOf(dataset), new StreamObserver<KafkaStreamElement>() {
      
      @Override
      public void onRegistered() {
        registerOperator(outputOp.get(), startLatch);
      }

      @Override
      public void onNext(int partitionId, KafkaStreamElement elem) {
        Writer<Object> writer = writers.get(partitionId);
        if (elem.isElement()) {
          try {
            writer.write(elem.getElement());
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
      }

      @Override
      public void onError(Throwable err) {
        writers.forEach(writer -> {
          try {
            writer.rollback();
            writer.close();
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        });
        finishLatch.countDown();
        LOG.error(
            "Error observing output stream of producer {}",
            dataset.getProducer(), err);
      }

      @Override
      public void onCompleted() {
        writers.forEach(writer -> {
          try {
            writer.flush();
            writer.commit();
            writer.close();
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        });
        finishLatch.countDown();
      }

    });
  }


  @SuppressWarnings("unchecked")
  private void consumeSource(Dataset<?> dataset, CountDownLatch latch) {

    final DataSource<?> source = Objects.requireNonNull(
        dataset.getSource(), "Root node in DAG always has to have source");
    final List<Partition<?>> partitions = (List<Partition<?>>) source.getPartitions();
    final List<Pair<BlockingQueue<KafkaStreamElement>, Integer>> outputs;
    final BlockingQueueObservableStream<KafkaStreamElement> outputStream;

    outputs = createOutputQueues(source.getPartitions().size());
    outputStream = BlockingQueueObservableStream.wrap(
        executor,
        "output-" + dataset.getProducer(),
        outputs);
    context.register(dataset, outputStream);

    AtomicInteger finished = new AtomicInteger(0);
    CountDownLatch consumersLatch = new CountDownLatch(partitions.size());
    // FIXME: this is wrong, need a way to balance the inputs here
    for (int partition = 0; partition < partitions.size(); partition++) {
      Partition<?> p = partitions.get(partition);
      BlockingQueue<KafkaStreamElement> output = outputs.get(partition).getFirst();
      executor.execute(() -> {
        consumersLatch.countDown();
        try {
          awaitLatch(latch);
          LOG.info(
              "Started thread consuming source {}, partition {}",
              source, p);
          Reader<?> reader = p.openReader();
          while (reader.hasNext()) {
            Object next = reader.next();
            output.put(KafkaStreamElement.FACTORY.data(
                next,
                Batch.BatchWindow.get(),
                System.currentTimeMillis()));
          }
        } catch (IOException ex) {
          LOG.error("Failed reading input {}", p, ex);
          throw new RuntimeException(ex);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
        if (finished.incrementAndGet() == partitions.size()) {
          outputs.forEach(o -> {
            try {
              o.getFirst().put(KafkaStreamElement.FACTORY.endOfStream());
            } catch (InterruptedException ex) {
              LOG.warn("Interrupted while waiting for stream ends.");
              Thread.currentThread().interrupt();
            }
          });
        }
      });
    }
    awaitLatch(consumersLatch);
    latch.countDown();
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

  private void closeStream(BlockingQueue<KafkaStreamElement> output) {    
    try {
      output.put(KafkaStreamElement.FACTORY.endOfStream());
    } catch (InterruptedException ex) {
      LOG.warn("Interrupted while finishing the calculation");
      Thread.currentThread().interrupt();
    }
  }

  private void registerOperator(Operator<?, ?> op, CountDownLatch latch) {
    if (latch.getCount() == 0) {
      throw new IllegalStateException(
          "Latch is already zero when trying to countdown during registration of operator "
              + op.getName());
    }
    latch.countDown();
    LOG.info("Started operator {}", op);
  }

  private KafkaProducer<byte[], byte[]> createProducer() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  private byte[] toBytes(KafkaStreamElement elem) {
    // FIXME: serialization
    return null;
  }

  private KafkaStreamElement fromBytes(byte[] bytes) {
    // FIXME: serialization
    return null;
  }

  protected java.util.concurrent.Executor getExecutor() {
    return executor;
  }

  private int numPartitions(int input, int partitioning) {
    return partitioning > 0 ? partitioning : input;
  }

}
