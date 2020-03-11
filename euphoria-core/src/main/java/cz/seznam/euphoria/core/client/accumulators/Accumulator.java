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
package cz.seznam.euphoria.core.client.accumulators;

import cz.seznam.euphoria.core.annotation.audience.Audience;

/**
 * Accumulators collect values from user functions.
 * Accumulators allow user to calculate statistics during the flow execution.
 * <p>
 * Accumulators are inspired by the Hadoop/MapReduce counters.
 */
@Audience(Audience.Type.INTERNAL)
public interface Accumulator {

}
