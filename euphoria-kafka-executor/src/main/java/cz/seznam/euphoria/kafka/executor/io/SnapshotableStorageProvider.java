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

package cz.seznam.euphoria.kafka.executor.io;

import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A storage provider holding data in memory, and committing to
 * configured file system.
 */
public class SnapshotableStorageProvider implements StorageProvider {

  /** The wrapped implementation of the provider. */
  final StorageProvider wrap;

  List<ListStorage<?>> createdListStorages;

  List<ValueStorage<?>> createdValueStorages;

  public SnapshotableStorageProvider(StorageProvider wrap) {
    this.wrap = wrap;
    this.createdListStorages = new ArrayList<>();
    this.createdValueStorages = new ArrayList<>();
  }

  /**
   * Store the storage into configured output directory.
   */
  @SuppressWarnings("unchecked")
  public void snapshot(File output) throws IOException {
    // FIXME: serialization
    int i = 0;
    for (ListStorage<?> s : createdListStorages) {
      String name = "list-storage-" + i++;
      File f = new File(output, name);
      try (OutputStream o = new FileOutputStream(f)) {
        storeStorage(s, o);
      }
    }
    i = 0;
    for (ValueStorage<?> s : createdValueStorages) {
      String name = "value-storage-" + i++;
      File f = new File(output, name);
      try (OutputStream o = new FileOutputStream(f)) {
        storeStorage(s, o);
      }
    }
  }

  /**
   * Load storage from stored snapshot.
   */
  @SuppressWarnings("unchecked")
  public void load(File path) throws IOException, ClassNotFoundException {
    int i = 0;

    for (ListStorage s : createdListStorages) {
      String name = "list-storage-" + i++;
      File f = new File(path, name);
      s.clear();
      if (f.exists()) {
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(f))) {
          Object read = in.readObject();
          while (read != null) {
            s.add(read);
            read = in.readObject();
          }
        }
      }
    }

    for (ValueStorage s : createdValueStorages) {
      String name = "value-storage-" + i++;
      File f = new File(path, name);
      if (f.exists()) {
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(f))) {
          Object read = in.readObject();
          s.set(read);
        }
      }
    }

  }

  @Override
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    ListStorage<T> listStorage = wrap.getListStorage(descriptor);
    createdListStorages.add(listStorage);
    return listStorage;
  }

  @Override
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    ValueStorage<T> valueStorage = wrap.getValueStorage(descriptor);
    createdValueStorages.add(valueStorage);
    return valueStorage;
  }

  private void storeStorage(ListStorage s, OutputStream o) throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(o)) {
      for (Object item : s.get()) {
        oos.writeObject(item);
      }
      oos.writeObject(null);
    }
  }

  private void storeStorage(ValueStorage s, OutputStream o) throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(o)) {
      oos.writeObject(s.get());
    }
  }

}
