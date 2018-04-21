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
package cz.seznam.euphoria.spark;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.internal.io.FileCommitProtocol;
import org.apache.spark.serializer.KryoRegistrator;
import scala.collection.immutable.Set;

public abstract class SparkKryoRegistrator implements KryoRegistrator  {

  @Override
  public void registerClasses(Kryo kryo) {
    kryo.register(FileCommitProtocol.TaskCommitMessage.class);
    kryo.register(Set.EmptySet$.class);
    registerUserClasses(kryo);
  }

  abstract protected void registerUserClasses(Kryo kryo);
}
