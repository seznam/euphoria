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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.partitioning.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioner;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Preconditions;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Lists;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Derived(
    state = StateComplexity.CONSTANT,
    repartitions = 1
)
public class Sort<
        IN, S extends Comparable<S>, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, IN, Integer, IN, W,
    Sort<IN, S, W>> {
  
  private static final class Sorted<V>
      extends State<V, V>
      implements StateSupport.MergeFrom<Sorted<V>> {

    @SuppressWarnings("unchecked")
    static final ListStorageDescriptor SORT_STATE_DESCR =
        ListStorageDescriptor.of("sort", (Class) Object.class);

    final ListStorage<V> curr;
    final Comparator<V> cmp;
    
    @SuppressWarnings("unchecked")
    Sorted(Context<V> context, StorageProvider storageProvider, Comparator<V> cmp) {
      super(context);
      this.curr = (ListStorage<V>) storageProvider.getListStorage(SORT_STATE_DESCR);
      this.cmp = cmp;
    }
    
    @Override
    public void add(V element) {
      curr.add(element);
    }
    
    @Override
    public void flush() {
      List<V> toSort = Lists.newArrayList(curr.get());
      Collections.sort(toSort, cmp);
      toSort.forEach(getContext()::collect);
    }
    
    @Override
    public void close() {
      curr.clear();
    }
    
    @Override
    public void mergeFrom(Sorted<V> other) {
      for (V v : other.curr.get()) {
        add(v);
      }
    }
  }

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <IN> ByBuilder<IN> of(Dataset<IN> input) {
      return new ByBuilder<>(name, input);
    }
  }

  public static class ByBuilder<IN> {
    private final String name;
    private final Dataset<IN> input;

    ByBuilder(String name, Dataset<IN> input) {
      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
    }

    public <S extends Comparable<S>> WindowByBuilder<IN, S> by(UnaryFunction<IN, S> sortByFn) {
      return new WindowByBuilder<>(name, input, requireNonNull(sortByFn));
    }
  }

  public static class WindowByBuilder<IN, S extends Comparable<S>>
      extends PartitioningBuilder<S, WindowByBuilder<IN, S>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<IN>
  {
    private String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, S> sortByFn;

    WindowByBuilder(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, S> sortByFn)
    {
      super(new DefaultPartitioning<>(input.getNumPartitions()));

      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.sortByFn = requireNonNull(sortByFn);
    }

    public <W extends Window>
    OutputBuilder<IN, S, W>
    windowBy(Windowing<IN, W> windowing) {
      return windowBy(windowing, null);
    }

    public <W extends Window>
    OutputBuilder<IN, S, W>
    windowBy(Windowing<IN, W> windowing, ExtractEventTime<IN> eventTimeAssigner) {
      return new OutputBuilder<>(name, input,
              sortByFn, this, requireNonNull(windowing), eventTimeAssigner);
    }

    @Override
    public Dataset<IN> output() {
      return new OutputBuilder<>(
          name, input, sortByFn, this, null, null).output();
    }
  }

  public static class OutputBuilder<
      IN, S extends Comparable<S>, W extends Window>
      extends PartitioningBuilder<S, OutputBuilder<IN, S, W>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<IN>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, S> sortByFn;
    @Nullable
    private final Windowing<IN, W> windowing;
    @Nullable
    private final ExtractEventTime<IN> eventTimeAssigner;

    OutputBuilder(String name,
                  Dataset<IN> input,
                  UnaryFunction<IN, S> sortByFn,
                  PartitioningBuilder<S, ?> partitioning,
                  @Nullable Windowing<IN, W> windowing,
                  @Nullable ExtractEventTime<IN> eventTimeAssigner) {

      super(partitioning);

      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.sortByFn = requireNonNull(sortByFn);
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    public Dataset<IN> output() {
      Preconditions.checkArgument(validPartitioning(getPartitioning()),
          "Non-single partitioning with default partitioner is not supported on Sort operator. "
          + "Set single partition or define custom partitioner - probably RangePartitioner?");
      Flow flow = input.getFlow();
      Sort<IN, S, W> top =
          new Sort<>(flow, name, input,
                  sortByFn, getPartitioning(), windowing, eventTimeAssigner);
      flow.add(top);
      return top.output();
    }

    private static boolean validPartitioning(Partitioning<?> partitioning) {
      return !partitioning.hasDefaultPartitioner() || partitioning.getNumPartitions() == 1;
    }
  }

  public static <I> ByBuilder<I> of(Dataset<I> input) {
    return new ByBuilder<>("Sort", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  // ~ -----------------------------------------------------------------------------

  private final UnaryFunction<IN, S> sortByFn;

  Sort(Flow flow,
            String name,
            Dataset<IN> input,
            UnaryFunction<IN, S> sortByFn,
            Partitioning<S> partitioning,
            @Nullable Windowing<IN, W> windowing,
            @Nullable ExtractEventTime<IN> eventTimeAssigner) {
    super(name, flow, input, 
        new PartitionKeyExtractor<>(sortByFn, partitioning), 
        windowing, eventTimeAssigner, 
        new HashPartitioning<>(partitioning.getNumPartitions()));
    
    this.sortByFn = sortByFn;
  }

  public UnaryFunction<IN, S> getSortByExtractor() {
    return sortByFn;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = getFlow();
    
    StateSupport.MergeFromStateMerger<IN, IN, Sorted<IN>>
            stateCombiner = new StateSupport.MergeFromStateMerger<>();
    ReduceStateByKey<IN, IN, IN, Integer, IN, Integer, IN, Sorted<IN>, W>
        reduce =
        new ReduceStateByKey<>(getName() + "::ReduceStateByKey", flow, input,
                keyExtractor,
                e -> e,
                windowing,
                eventTimeAssigner,
                (StateFactory<IN, IN, Sorted<IN>>)
                    (ctx, provider) -> new Sorted<>(ctx, provider, new SortByComparator<>(sortByFn)),
                stateCombiner,
                partitioning);

    MapElements<Pair<Integer, IN>, IN>
        format =
        new MapElements<>(getName() + "::MapElements", flow, reduce.output(),
            e -> e.getSecond());

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);
    return dag;
  }
  
  private static class SortByComparator<V, S extends Comparable<S>> implements Comparator<V>, Serializable {

    private final UnaryFunction<V, S> sortByFn;
    
    public SortByComparator(UnaryFunction<V, S> sortByFn) {
      this.sortByFn = sortByFn;
    }

    @Override
    public int compare(V o1, V o2) {
      return sortByFn.apply(o1).compareTo(sortByFn.apply(o2));
    }
  }
  
  private static class PartitionKeyExtractor<IN, S extends Comparable<S>> implements UnaryFunction<IN, Integer> {

    private final UnaryFunction<IN, S> sortByFn;
    private final Partitioner<S> partitioner;
    private final int numPartitions;
    
    public PartitionKeyExtractor(UnaryFunction<IN, S> sortByFn, Partitioning<S> partitioning) {
      this.sortByFn = sortByFn;
      this.partitioner = partitioning.getPartitioner();
      this.numPartitions = partitioning.getNumPartitions();
    }

    @Override
    public Integer apply(IN what) {
      int ret = partitioner.getPartition(sortByFn.apply(what)) % numPartitions;
      return ret;
    }
  }
}
