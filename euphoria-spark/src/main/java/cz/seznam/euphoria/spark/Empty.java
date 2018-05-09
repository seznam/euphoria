package cz.seznam.euphoria.spark;

import java.io.Serializable;

/**
 * Empty value singleton.
 */
public class Empty implements Serializable {

  private static final Empty INSTANCE = new Empty();

  /**
   * Get instance
   *
   * @return instance
   */
  public static Empty get() {
    return INSTANCE;
  }

  private Empty() {

  }
}
