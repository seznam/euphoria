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
package cz.seznam.euphoria.core.client.functional;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.io.Context;

import java.io.Serializable;

/**
 * Function of single argument with access to Euphoria environment via context.
 *
 * @param <IN> the type of the element processed
 * @param <OUT> the type of the result applying element to the function
 */
@Audience(Audience.Type.CLIENT)
@FunctionalInterface
public interface UnaryFunctionEnv<IN, OUT> extends Serializable {

  /**
   * Applies function to given element.
   *
   * @param what The element applied to the function
   * @param context Provides access to the environment.
   *
   * @return the result of the function application
   */
  OUT apply(IN what, Context context);

}
