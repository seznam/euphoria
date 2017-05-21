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

import com.google.protobuf.ByteString;
import cz.seznam.euphoria.core.annotation.stability.Experimental;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioner;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
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
import cz.seznam.euphoria.kafka.executor.io.Serde.Serializer;
import cz.seznam.euphoria.kafka.executor.io.SnapshotableStorageProvider;
import cz.seznam.euphoria.kafka.executor.io.TopicSpec;
import cz.seznam.euphoria.kafka.executor.proto.Serialization;
import cz.seznam.euphoria.shaded.guava.com.google.common.annotations.VisibleForTesting;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Joiner;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@code Executor} passing data between pipelines via Apache Kafka topics.
 */
@Experimental(
    "Consider this class as experimental PoC. In the future it will be either "
        + "removed or marked as stable.")
public class KafkaExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaExecutor.class);

  // FIXME: this has to be shared across a cluster and from interval (0, N - 1)
  // use akka to distribute this acroos multiple nodes
  private final int globalId = 0;
  private final ExecutorService executor;
  private final String[] bootstrapServers;
  private final ExecutionContext context = new ExecutionContext();
  final BiFunction<Flow, Dataset<?>, TopicSpec<? extends Window, ?>> topicGenerator;


  public KafkaExecutor(
      ExecutorService executor,
      String[] bootstrapServers,
      BiFunction<Flow, Dataset<?>, TopicSpec<? extends Window, ?>> topicGenerator) {

    this.executor = executor;
    this.bootstrapServers = bootstrapServers;
    this.topicGenerator = topicGenerator;
  }

  @Override
  public CompletableFuture<Result> submit(Flow flow) {
    return CompletableFuture.supplyAsync(() -> execute(flow), executor);
  }

  @Override
  public void shutdown() {
    executor.shutdown();
  }

  private Result execute(Flow flow) {
    DAG<Operator<?, ?>> unfolded = FlowUnfolder.unfold(flow, getBasicOps());

    try {
      validateTopics(flow, unfolded);
    } catch (Exception ex) {
      LOG.error("Failed validation of flow {}", flow, ex);
      return new Result();
    }

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
    executor.shutdownNow();
    return new Result();
  }

  private Set<Class<? extends Operator<?, ?>>> getBasicOps() {
    // use the "standard" set of basic operators
    return Executor.getBasicOps();
  }

  // verify that all topics exist
  @VisibleForTesting
  void validateTopics(Flow flow, DAG<Operator<?, ?>> dag) {
    ZkClient client = new ZkClient("localhost:2181");
    try {
      ZkConnection conn = new ZkConnection("localhost:2181");
      ZkUtils utils = new ZkUtils(client, conn, true);
      List<String> unknownTopics = dag.traverse()
          .filter(n -> n.get() instanceof Repartition || n.get() instanceof ReduceStateByKey)
          .map(n -> topicGenerator.apply(flow, n.getSingleParent().get().output()).getName())
          .filter(t -> !AdminUtils.topicExists(utils, t))
          .collect(Collectors.toList());
      if (!unknownTopics.isEmpty()) {
        throw new IllegalArgumentException(
            "Cannot run the flow. Missing topics " + unknownTopics);
      }
    } finally {
      client.close();
    }
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
    final List<BlockingQueue<KafkaStreamElement>> outputs;
    ExtractEventTime eventTimeExtractor = op.getEventTimeExtractor();

    outputs = Collections.synchronizedList(new ArrayList<>());
    createOutputQueues(outputs, stream.size());
    context.register(op.output(), BlockingQueueObservableStream.wrap(
        executor, op.getName(), outputs));

    List<MapContext> contexts = new ArrayList();
    for (int i = 0; i < stream.size(); i++) {
      contexts.add(new MapContext(outputs.get(i)));
    }

    stream.observe(new StreamObserver<KafkaStreamElement>() {

      @Override
      public void onAssign(int numPartitions) {
        registerOperator(op, latch);
        awaitLatch(latch);
      }

      @Override
      public void onNext(int partitionId, KafkaStreamElement elem) {
        MapContext mapContext = contexts.get(partitionId);
        BlockingQueue<KafkaStreamElement> output = outputs.get(partitionId);
        if (elem.isElement()) {
          if (eventTimeExtractor != null) {
            elem.reassignTimestamp(
                eventTimeExtractor.extractTimestamp(elem.getElement()));
          }
          mapContext.setInput(elem);
          functor.apply(elem.getElement(), mapContext);
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
      }

      @Override
      public void onCompleted() {
        outputs.forEach(o -> {
          try {
            o.put(KafkaStreamElement.FACTORY.endOfStream());
          } catch (InterruptedException ex) {
            LOG.warn("Interrupted while sending EOS to downstream operators.");
          }
        });
      }
    });
  }

  private void repartition(
      Flow flow,
      Node<Operator<?, ?>> node,
      Repartition<?> op,
      CountDownLatch latch) {

    Dataset<?> input = Iterables.getOnlyElement(node.getParents()).get().output();
    ObservableStream<KafkaStreamElement> stream = context.get(input);
    Partitioning<?> partitioning = op.getPartitioning();

    Partitioner partitioner = partitioning.getPartitioner();

    final List<BlockingQueue<KafkaStreamElement>> outputs;
    final OutputWriter writer = outputWriter(flow, op.output());
    final int numPartitions = numPartitions(
        writer.numPartitions(),
        partitioning.getNumPartitions());

    outputs = Collections.synchronizedList(new ArrayList<>());
    createOutputQueues(outputs, numPartitions);

    context.register(op.output(), BlockingQueueObservableStream.wrap(
        executor, op.getName(), outputs));

    ObservableStream<KafkaStreamElement> repartitioned;
    repartitioned = kafkaObservableStream(flow, op.output());

    CountDownLatch runningParts = new CountDownLatch(2);

    stream.observe(
        sendingObserver(
            op, runningParts,
            e -> e,
            partitioner, numPartitions,
            Optional.empty(),
            writer));

    repartitioned.observe(
        receivingObserver(
            op, runningParts,
            latch, outputs,
            new WatermarkEmitStrategy.Default(),
            writer::close));

    awaitLatch(runningParts);
    LOG.info("Started both ends of repartition operator {}", op.getName());
    latch.countDown();

  }

  @SuppressWarnings("unchecked")
  private StreamObserver<KafkaStreamElement> receivingObserver(
      Operator op,
      CountDownLatch runningParts,
      CountDownLatch latch,
      List<BlockingQueue<KafkaStreamElement>> outputQueues,
      WatermarkEmitStrategy emitStrategy,
      Runnable onClose) {

    return new StreamObserver<KafkaStreamElement>() {

      AtomicLong watermark = new AtomicLong(0);
      AtomicReference<VectorClock> clock = new AtomicReference<>();
      
      {
        emitStrategy.schedule(() -> {
          long stamp = watermark.get();
          outputQueues.forEach(output -> {
            try {
              output.put(KafkaStreamElement.FACTORY.watermark(stamp));
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            }
          });
          LOG.debug("Updated watermark of downstream queues to {}", stamp);
        });
      }


      @Override
      public void onAssign(int numPartitions) {
        runningParts.countDown();
        clock.set(new VectorClock(numPartitions));
        awaitLatch(latch);
        // send downstream command to reset the state
        /* for (BlockingQueue<KafkaStreamElement> out : outputQueues) {
          try {
            out.put(KafkaStreamElement.FACTORY.rewindStateToCommitted());
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        } */
        LOG.info(
            "Started receiving part of operator {} with {} assigned partitions",
            op.getName(),
            numPartitions);
      }

      @Override
      public void onNext(int partitionId, KafkaStreamElement elem) {

        try {
          if (elem.isElement() || elem.isWindowTrigger() || elem.isRewindState()) {
            BlockingQueue<KafkaStreamElement> output;
            output = outputQueues.get(partitionId);
            output.put(elem);
          }
          clock.get().update(elem.getTimestamp(), partitionId);
          watermark.set(clock.get().getCurrent());
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
            output.put(KafkaStreamElement.FACTORY.endOfStream());
            onClose.run();
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
      UnaryFunction keyExtractor,
      Partitioner partitioner,
      int numPartitions,
      Optional<WatermarkEmitStrategy> emitStrategy,
      OutputWriter writer) {

    Map<Integer, AtomicLong> watermarks = new ConcurrentHashMap<>();
    runningParts.countDown();
    if (emitStrategy.isPresent()) {
      emitStrategy.get().schedule(() -> {
        watermarks.entrySet().forEach(entry -> {
          int source = entry.getKey();
          for (int target = 0; target < numPartitions; target++) {
            writer.write(KafkaStreamElement.FACTORY.watermark(entry.getValue().get()),
                source, target, (succ, exc) -> {
                  // FIXME: error handling
                  if (!succ) {
                    throw new RuntimeException(exc);
                  }
                });
            };
        });
      });
    }
    LOG.info("Started sending part of operator {}", op.getName());

    return new StreamObserver<KafkaStreamElement>() {

      @Override
      public void onNext(int source, KafkaStreamElement elem) {
        if (elem.isElement()) {
          AtomicLong watermark = watermarks.get(source);
          if (watermark == null) {
            watermarks.put(source, watermark = new AtomicLong());
          }
          watermarks.putIfAbsent(source, new AtomicLong());
          long stamp = elem.getTimestamp();
          watermark.accumulateAndGet(stamp, (x, y) -> x < y ? y : x);
          int target = (partitioner.getPartition(
              keyExtractor.apply(elem.getElement())) & Integer.MAX_VALUE)
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
    List<ObservableStream<KafkaStreamElement>> streams = inputs.stream()
        .map(dataset -> context.get(dataset))
        .collect(Collectors.toList());

    final List<BlockingQueue<KafkaStreamElement>> outputs;
    final BlockingQueueObservableStream<KafkaStreamElement> outputStream;

    outputs = Collections.synchronizedList(new ArrayList<>());
    createOutputQueues(outputs, op.output().getNumPartitions());

    context.register(op.output(), BlockingQueueObservableStream.wrap(
        executor, op.getName(), outputs));
    CountDownLatch partitionsLatch = new CountDownLatch(streams.size());
    VectorClock clock = new VectorClock(streams.size());
    AtomicInteger finished = new AtomicInteger(0);

    streams.forEach(p -> {
      p.observe(new StreamObserver<KafkaStreamElement>() {

        AtomicLong watermark = new AtomicLong();
        WatermarkEmitStrategy emitStrategy = new WatermarkEmitStrategy.Default();

        @Override
        public void onAssign(int numPartitions) {
          partitionsLatch.countDown();
          emitStrategy.schedule(() -> {
            KafkaStreamElement elem = KafkaStreamElement.FACTORY.watermark(
                watermark.get());
            outputs.forEach(output -> {
              try {
                output.put(elem);
              } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
              }
            });
          });
          LOG.info("Started operator {}", op.getName());
          awaitLatch(latch);
        }

        @Override
        public void onNext(int partitionId, KafkaStreamElement elem) {
          BlockingQueue<KafkaStreamElement> output = outputs.get(partitionId);
          try {
            if (elem.isElement() || elem.isWindowTrigger() || elem.isRewindState()) {
              // just blindly forward to output
              output.put(elem);
            } else if (elem.isWatermark()) {
              clock.update(elem.getTimestamp(), partitionId);
              watermark.set(clock.getCurrent());
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
                  String.valueOf(p),
                  String.valueOf(total),
                  String.valueOf(streams.size())
                });
          }

          if (total == streams.size()) {
            outputs.forEach(output -> {
              try {
                output.put(KafkaStreamElement.FACTORY.endOfStream());
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
      ReduceStateByKey<?, ?, ?, ?, ?, ?> op,
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
    final List<BlockingQueue<KafkaStreamElement>> outputs;
    final List<BlockingQueue<KafkaStreamElement>> repartitions;

    BlockingQueueObservableStream<KafkaStreamElement> repartitionStream;
    BlockingQueueObservableStream<KafkaStreamElement> outputStream;
    OutputWriter repartitionWriter = outputWriter(flow, op.output());
    final int numPartitions = numPartitions(
        repartitionWriter.numPartitions(),
        partitioning.getNumPartitions());

    ObservableStream<KafkaStreamElement> repartitioned;

    outputs = Collections.synchronizedList(new ArrayList<>());
    repartitions = Collections.synchronizedList(new ArrayList<>());

    createOutputQueues(outputs, numPartitions);
    createOutputQueues(repartitions, numPartitions);

    repartitioned = kafkaObservableStream(flow, op.output());
    repartitionStream = BlockingQueueObservableStream.wrap(
        executor, op.getName(), repartitions);
    context.register(op.output(), BlockingQueueObservableStream.wrap(
        executor, op.getName(), outputs));

    CountDownLatch parts = new CountDownLatch(3);

    List<ReduceStateByKeyReducer> reducers = new ArrayList<>();
    List<SnapshotableStorageProvider> storageProviders = new ArrayList<>();
    for (int p = 0; p < numPartitions; p++) {
      BlockingQueue<KafkaStreamElement> output = outputs.get(p);
      // FIXME: specify storage provider
      storageProviders.add(new SnapshotableStorageProvider(
          new InMemStorageProvider()));
      ReduceStateByKeyReducer reducer = new ReduceStateByKeyReducer(
          op, op.getName(),
          collector(output),
          keyExtractor,
          valueExtractor,
          // FIXME: specify scheduler from client code
          op.input().isBounded()
              ? new NoopTriggerScheduler()
              : (windowing != null
                  ? new WatermarkTriggerScheduler(500)
                  : new WatermarkTriggerScheduler(500)),
          // FIXME; specify watermark emit strategy
          new WatermarkEmitStrategy.Default(),
          storageProviders.get(p),
          // FIXME: configurable
          false,
          KafkaStreamElement.FACTORY);

      reducer.setup();
      reducers.add(reducer);
    }

    stream.observe(sendingObserver(
            op, parts, keyExtractor, partitioner,
            numPartitions,
            Optional.of(new WatermarkEmitStrategy.Default()),
            repartitionWriter));

    repartitioned.observe(
        receivingObserver(
            op, parts, latch, repartitions,
            new WatermarkEmitStrategy.Default(),
            repartitionWriter::close));


    repartitionStream.observe(reduceStream(
        op, parts, latch, reducers, outputs, storageProviders));

    awaitLatch(parts);
    LOG.info("Started all parts of RSBK {}", op.getName());
    latch.countDown();
  }

  private StreamObserver<KafkaStreamElement> reduceStream(
      ReduceStateByKey op,
      CountDownLatch parts,
      CountDownLatch latch,
      List<ReduceStateByKeyReducer> reducers,
      List<BlockingQueue<KafkaStreamElement>> outputs,
      List<SnapshotableStorageProvider> states) {

    return new StreamObserver<KafkaStreamElement>() {
      @Override
      public void onAssign(int numPartitions) {
        parts.countDown();
        LOG.info("Started reducing part of operator {}", op.getName());
      }
      @Override
      public void onNext(int partitionId, KafkaStreamElement elem) {
        // ensure we will write the result into the correct output partition
        if (elem.isRewindState()) {
          LOG.info("Rewinding states to last committed version");
          states.forEach(s -> {
            try {
              s.load(new File("./snapshots-" + s));
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
          });
          try {
            outputs.get(partitionId).put(elem);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        } else {
          reducers.get(partitionId).accept(elem);
        }
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
      Flow flow,
      Dataset<?> dataset) {

    return new KafkaObservableStream(
        executor,bootstrapServers,
        topicGenerator.apply(flow, dataset),
        nameForConsumerOf(dataset));
  }

  /**
   * Create writer for given dataset.
   */
  @SuppressWarnings("unchecked")
  @VisibleForTesting
  OutputWriter outputWriter(Flow flow, Dataset<?> output) {
    KafkaProducer<byte[], byte[]> producer = createProducer();
    TopicSpec<Window, Object> topicSpec = (TopicSpec) topicGenerator.apply(flow, output);
    Serializer<Window> windowSerializer = topicSpec.getWindowSerialization().serializer();
    Serializer<Object> payloadSerializer = topicSpec.getPayloadSerialization().serializer();

    return new OutputWriter() {

      @Override
      public int numPartitions() {
        return producer.partitionsFor(topicSpec.getName()).size();
      }

      @Override
      public void write(
          KafkaStreamElement elem, int source, int target, Callback callback) {
        try {
          // FIXME: serialization
          ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
              topicSpec.getName(), target, elem.getTimestamp(),
              Serialization.Source.newBuilder().setSource(source).build().toByteArray(),
              toBytes(windowSerializer, payloadSerializer, elem));
          producer.send(record, (meta, exc) -> callback.apply(exc == null, exc));
        } catch (Exception ex) {
          LOG.error("Failed to send {}", elem, ex);
        }
      }

      @Override
      public void close() {
        producer.close();
      }

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
  void observeOutput(
      Node<Operator<?, ?>> outputOp,
      CountDownLatch startLatch,
      CountDownLatch finishLatch) {

    Dataset<?> dataset = outputOp.get().output();
    ObservableStream<KafkaStreamElement> outputs = context.get(dataset);
    DataSink<?> sink = Objects.requireNonNull(
        dataset.getOutputSink(),
        "Missing datasink of leaf dataset " + dataset
            + ", producer " + dataset.getProducer());

    sink.initialize();
    List<Writer<Object>> writers = new ArrayList<>(dataset.getNumPartitions());
    for (int p = 0; p < dataset.getNumPartitions(); p++) {
      writers.add((Writer) sink.openWriter(p));
    }

    outputs.observe(new StreamObserver<KafkaStreamElement>() {

      @Override
      public void onAssign(int numPartitions) {
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
    final List<BlockingQueue<KafkaStreamElement>> outputs;

    outputs = Collections.synchronizedList(new ArrayList<>());
    createOutputQueues(outputs, source.getPartitions().size());
    context.register(dataset, BlockingQueueObservableStream.wrap(
        executor, source.toString(), outputs));

    AtomicInteger finished = new AtomicInteger(0);
    CountDownLatch consumersLatch = new CountDownLatch(partitions.size());
    // FIXME: this is wrong, need a way to balance the inputs here
    for (int partition = 0; partition < partitions.size(); partition++) {
      Partition<?> p = partitions.get(partition);
      BlockingQueue<KafkaStreamElement> output = outputs.get(partition);
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
                GlobalWindowing.Window.get(),
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
              o.put(KafkaStreamElement.FACTORY.endOfStream());
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
      if (!latch.await(60, TimeUnit.SECONDS)) {
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
    latch.countDown();
    LOG.info("Started operator {}", op);
  }

  private KafkaProducer<byte[], byte[]> createProducer() {
    Properties props = new Properties();
    props.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        Joiner.on(",").join(bootstrapServers));
    return new KafkaProducer<>(
        props,
        Serdes.ByteArray().serializer(),
        Serdes.ByteArray().serializer());
  }

  private byte[] toBytes(
      Serializer<Window> windowSerializer,
      Serializer<Object> payloadSerializer,
      KafkaStreamElement elem) {

    Serialization.Element.Type type = toProtoType(elem);
    Serialization.Element.Builder builder = Serialization.Element.newBuilder()
        .setType(type);
    switch (type) {
      case ELEMENT:
        builder.setPayload(ByteString.copyFrom(payloadSerializer.apply(elem.getElement())));
      case WINDOW_TRIGGER:
        builder.setWindow(ByteString.copyFrom(windowSerializer.apply(elem.getWindow())));
        break;
    }
    return builder.build().toByteArray();
  }

  protected java.util.concurrent.Executor getExecutor() {
    return executor;
  }

  private int numPartitions(int writer, int partitioning) {
    if (partitioning > 0) {
      if (writer < partitioning) {
        throw new IllegalStateException(
            "Need topic with at least " + partitioning + " partitions!");
      }
      return partitioning;
    }
    return writer;
  }

  void createOutputQueues(List<BlockingQueue<KafkaStreamElement>> queues, int count) {
    for (int i = 0; i < count; i++) {
      queues.add(new ArrayBlockingQueue<>(100));
    }
  }


  // retrieve topic that the given dataset should be stored into
  private static String topicForDataset(Flow flow, Dataset<?> dataset) {
    return "__euphoria_"
        + flow.getName() + "_"
        + Optional.ofNullable(dataset.getProducer())
            .map(op -> op.getName())
            .orElse("_input_" + dataset.getSource());
  }

  private Serialization.Element.Type toProtoType(KafkaStreamElement elem) {
    if (elem.isElement())
      return Serialization.Element.Type.ELEMENT;
    if (elem.isWatermark())
      return Serialization.Element.Type.WATERMARK;
    if (elem.isWindowTrigger())
      return Serialization.Element.Type.WINDOW_TRIGGER;
    if (elem.isEndOfStream())
      return Serialization.Element.Type.END_OF_STREAM;
    return Serialization.Element.Type.UNRECOGNIZED;
  }

}
