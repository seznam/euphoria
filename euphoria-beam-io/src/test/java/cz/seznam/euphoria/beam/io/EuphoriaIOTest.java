/*
 * Copyright 2016-2019 Seznam.cz, a.s.
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
package cz.seznam.euphoria.beam.io;

import cz.seznam.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;

public class EuphoriaIOTest {

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void boundedReadTest() {

    List<String> inputList = asList(
        "one", "two", "three", "four", "five",
        "one two three four four two two",
        "one one one two two three");
    ListDataSource<String> input = ListDataSource.bounded(inputList);

    PCollection<String> output = testPipeline.apply(EuphoriaIO.read(input, StringUtf8Coder.of()));

    PAssert.that(output).containsInAnyOrder(inputList);

    testPipeline.run().waitUntilFinish();

  }
}
