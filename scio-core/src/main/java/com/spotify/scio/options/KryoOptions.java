/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.options;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.common.collect.ImmutableList;
import com.twitter.chill.IKryoRegistrar;
import com.twitter.chill.algebird.AlgebirdRegistrar;

import java.util.List;

@Description("Internal options for Kryo")
public interface KryoOptions extends PipelineOptions {
  @Description("List of Kryo registrars")
  @Default.InstanceFactory(KryoRegistrarsFactory.class)
  List<Class<? extends IKryoRegistrar>> getKryoRegistrars();
  void setKryoRegistrars(List<Class<? extends IKryoRegistrar>> registrars);

  public static class KryoRegistrarsFactory
      implements DefaultValueFactory<List<Class<? extends IKryoRegistrar>>> {
    @Override
    public List<Class<? extends IKryoRegistrar>> create(PipelineOptions options) {
      return ImmutableList.<Class<? extends IKryoRegistrar>>of(AlgebirdRegistrar.class);
    }
  }
}
