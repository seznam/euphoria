package cz.seznam.euphoria.core.client.operator;

import java.io.Serializable;

@FunctionalInterface
public interface ExtractEventTime<I> extends Serializable {

  /** Extracts event time (in millis since epoch) of the given element. */
  long extractTimestamp(I elem);
}
