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

import com.google.common.collect.Lists;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.inmem.InMemStorageProvider;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Test suite for {@code SnapshotableStorageProviderTest}.
 */
public class SnapshotableStorageProviderTest {

  SnapshotableStorageProvider provider;
  Path path;

  @Before
  public void setUp() throws IOException {
    provider = new SnapshotableStorageProvider(new InMemStorageProvider());
    path = FileSystems.getDefault().getPath("./tmp");
    path.getFileSystem().provider().createDirectory(path);
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(path.toFile());
  }

  @Test
  public void testListSnapshot() throws IOException, ClassNotFoundException {
    ListStorage<Integer> storage = provider.getListStorage(
        ListStorageDescriptor.of("my-storage", Integer.class));

    storage.add(1);
    storage.add(2);

    assertEquals(Arrays.asList(1, 2), Lists.newArrayList(storage.get()));

    provider.snapshot(path.toFile());
    storage.add(3);
    assertEquals(Arrays.asList(1, 2, 3), Lists.newArrayList(storage.get()));
    provider.load(path.toFile());

    assertEquals(Arrays.asList(1, 2), Lists.newArrayList(storage.get()));
  }

  @Test
  public void testValueSnapshot() throws IOException, ClassNotFoundException {
    ValueStorage<Integer> storage = provider.getValueStorage(
        ValueStorageDescriptor.of("my-storage", Integer.class, 0));

    storage.set(1);

    assertEquals(1, (int) storage.get());

    provider.snapshot(path.toFile());
    storage.set(2);
    assertEquals(2, (int) storage.get());
    provider.load(path.toFile());

    assertEquals(1, (int) storage.get());
  }

  @Test
  public void testLoadOnNoFile() throws IOException, ClassNotFoundException {
    ListStorage<Integer> storage = provider.getListStorage(
        ListStorageDescriptor.of("my-storage", Integer.class));

    storage.add(1);
    provider.load(path.toFile());

    assertTrue(Iterables.isEmpty(storage.get()));
  }

}
