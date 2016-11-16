package cz.seznam.euphoria.operator.test.ng.junit;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class AbstractOperatorTest implements Serializable {

  /** Automatically injected if the test class or the a suite it is part of
   * is annotated with {@code @RunWith(ExecutorProviderRunner.class)}.
   */
  protected transient Executor executor;

  /**
   * A single test case.
   */
  protected interface TestCase<T> extends Serializable {

    /** Retrieve number of output partitions to expect in output. */
    int getNumOutputPartitions();

    /** Retrieve flow to be run. Write outputs to given sink. */
    Dataset<T> getOutput(Flow flow);

    /** Validate outputs. */
    void validate(List<List<T>> partitions);

    /** Retrieve number of runs for the test. */
    default int getNumRuns() { return 1; }

  }

  /**
   * Abstract {@code TestCase} to be extended by test classes.
   */
  public static abstract class AbstractTestCase<I, O> implements TestCase<O> {

    final protected Flow flow;
    final protected Settings settings;

    private static String getCallerName() {
      StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
      if (stackTrace.length > 4) {
        return stackTrace[4].getMethodName();
      }
      return "UNKNOWN";
    }

    protected AbstractTestCase() {
      this(getCallerName());
    }

    protected AbstractTestCase(Settings settings) {
      this(getCallerName(), settings);
    }

    protected AbstractTestCase(String name) {
      this(name, new Settings());
    }

    protected AbstractTestCase(String name, Settings settings) {
      this.flow = Flow.create(name, settings);
      this.settings = settings;
    }

    @Override
    @SuppressWarnings("unchecked")
    public final Dataset<O> getOutput(Flow flow) {
      Dataset<I> input = flow.createInput(getDataSource());
      Dataset<O> output = getOutput(input);
      return output;
    }

    protected abstract Dataset<O> getOutput(Dataset<I> input);

    protected abstract DataSource<I> getDataSource();
  }

  /**
   * Run all tests with given executor.
   */
  @SuppressWarnings("unchecked")
  public void execute(TestCase tc) throws Exception {
    for (int i = 0; i < tc.getNumRuns(); i++) {
      ListDataSink sink = ListDataSink.get(tc.getNumOutputPartitions());
      Flow flow = Flow.create(tc.toString());
      tc.getOutput(flow).persist(sink);
      executor.submit(flow).get();
      tc.validate(sink.getOutputs());
    }
  }

  protected static <T> void assertUnorderedEquals(
      String message, List<T> first, List<T> second) {
    Map<T, Integer> firstSet = countMap(first);
    Map<T, Integer> secondSet = countMap(second);
    if (message != null) {
      assertEquals(message, firstSet, secondSet);
    } else {
      assertEquals(firstSet, secondSet);
    }
  }

  protected static <T> void assertUnorderedEquals(
      List<T> first, List<T> second) {
    assertUnorderedEquals(null, first, second);
  }

  private static <T> Map<T, Integer> countMap(List<T> list) {
    Map<T, Integer> ret = new HashMap<>();
    list.forEach(e -> {
      Integer current = ret.get(e);
      if (current == null) {
        ret.put(e, 1);
      } else {
        ret.put(e, current + 1);
      }
    });
    return ret;
  }

}