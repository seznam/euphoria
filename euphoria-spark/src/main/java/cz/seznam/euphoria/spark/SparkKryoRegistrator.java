/*
 * Copyright 2016-2020 Seznam.cz, a.s.
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
package cz.seznam.euphoria.spark;

import com.esotericsoftware.kryo.Kryo;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.spark.kryo.SingletonSerializer;
import org.apache.spark.internal.io.FileCommitProtocol;
import org.apache.spark.serializer.KryoRegistrator;

import java.util.HashMap;

/**
 * Base class for user specific kryo settings.
 */
public abstract class SparkKryoRegistrator implements KryoRegistrator {

  @Override
  public void registerClasses(Kryo kryo) {
    try {
    // windows
    kryo.register(Window.class);
    kryo.register(TimeInterval.class);
    kryo.register(GlobalWindowing.Window.class, SingletonSerializer.of("get"));

    // element wrappers
    kryo.register(KeyedWindow.class);
    kryo.register(ReduceByKeyTranslator.KeyedWindowValue.class);
    kryo.register(SparkElement.class);
    kryo.register(Either.class);
    kryo.register(BatchJoinKey.class);
    kryo.register(BatchJoinKey.Side.class);
    kryo.register(Empty.class, SingletonSerializer.of("get"));

    // broadcast hash join
    kryo.register(HashMap.class);

    // misc
    kryo.register(Pair.class);

    // inner spark classes, this should be fixed in spark itself
    kryo.register(FileCommitProtocol.TaskCommitMessage.class);
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"));

    registerUserClasses(kryo);

    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Unable to register classes with kryo.", e);
    }
  }

  /**
   * Register user specific classes with Kryo serialization framework.
   *
   * @param kryo instance
   */
  protected abstract void registerUserClasses(Kryo kryo);
}
