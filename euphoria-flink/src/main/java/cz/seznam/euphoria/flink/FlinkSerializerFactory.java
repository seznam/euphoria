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
package cz.seznam.euphoria.flink;

import com.esotericsoftware.kryo.Kryo;
import cz.seznam.euphoria.core.executor.io.SerializerFactory;
import java.io.OutputStream;
import java.util.HashSet;
import org.apache.flink.api.common.ExecutionConfig;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig.SerializableSerializer;

public class FlinkSerializerFactory implements SerializerFactory {

  static class FlinkSerializerAdapter implements Serializer {

    private final Kryo kryo;

    FlinkSerializerAdapter(Kryo kryo) {
      this.kryo = kryo;
    }

    @Override
    public SerializerFactory.Serializer.Output newOutput(OutputStream os) {

      com.esotericsoftware.kryo.io.Output output = new com.esotericsoftware.kryo.io.Output(os);
      return new SerializerFactory.Serializer.Output() {
        @SuppressWarnings("unchecked")
        @Override
        public void writeObject(Object element) {
          kryo.writeClassAndObject(output, element);
        }

        @Override
        public void flush() {
          output.flush();
        }

        @Override
        public void close() {
          output.close();
        }
      };
    }

    @Override
    public SerializerFactory.Serializer.Input newInput(java.io.InputStream is) {
      com.esotericsoftware.kryo.io.Input input = new com.esotericsoftware.kryo.io.Input(is);
      return new SerializerFactory.Serializer.Input() {
        @SuppressWarnings("unchecked")
        @Override
        public Object readObject() {
          return kryo.readClassAndObject(input);
        }

        @Override
        public boolean eof() {
          return input.eof();
        }

        @Override
        public void close() {
          input.close();
        }
      };
    }
  }

  private final Map<Class<?>, ExecutionConfig.SerializableSerializer<?>> serializers;
  private final Set<Class<?>> registeredClasses;
  private transient Kryo kryo;

  public FlinkSerializerFactory(ExecutionConfig conf) {
    serializers = instantiateSerializers(
        conf.getRegisteredTypesWithKryoSerializerClasses());
    serializers.putAll(instantiateSerializers(
        conf.getDefaultKryoSerializerClasses()));
    serializers.putAll(conf.getDefaultKryoSerializers());
    serializers.putAll(conf.getRegisteredTypesWithKryoSerializers());
    registeredClasses = new HashSet<>(conf.getRegisteredKryoTypes());
    registeredClasses.addAll(conf.getRegisteredPojoTypes());
  }

  @Override
  public Serializer newSerializer() {
    return new FlinkSerializerAdapter(initKryo());
  }

  private Kryo initKryo() {
    if (this.kryo == null) {
      this.kryo = new Kryo();
      ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
          .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
      serializers.forEach((k, v) -> kryo.addDefaultSerializer(k, v.getSerializer()));
      registeredClasses.forEach(kryo::register);
    }
    return this.kryo;
  }

  @SuppressWarnings("unchecked")
  private Map<Class<?>, SerializableSerializer<?>> instantiateSerializers(
        Map<Class<?>, Class<? extends com.esotericsoftware.kryo.Serializer<?>>> serializers) {

    return serializers.entrySet().stream().collect(
        Collectors.toMap(
            Map.Entry::getKey,
            e -> {
              try {
                return new SerializableSerializer(e.getValue().newInstance());
              } catch (InstantiationException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
              }
            }));
  }

}
