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

package com.spotify.scio.coders;

import com.esotericsoftware.kryo.Kryo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.spotify.scio.options.KryoOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public class KryoAtomicCoder<T> extends AtomicCoder<T> {

  private final KryoOptions kryoOptions;
  private final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
    @Override
    protected Kryo initialValue() {
      return KryoAtomicCoderUtil.newKryo(kryoOptions);
    }
  };

  // FIXME: remove this
  public static <T> KryoAtomicCoder<T> of() {
    return new KryoAtomicCoder<>(PipelineOptionsFactory.as(KryoOptions.class));
  }

  @JsonCreator
  public static <T> KryoAtomicCoder<T> of(@JsonProperty("options") KryoOptions kryoOptions) {
    return new KryoAtomicCoder<>(kryoOptions);
  }

  protected KryoAtomicCoder(KryoOptions kryoOptions) {
    this.kryoOptions = kryoOptions;
  }

  public KryoOptions getKryoOptions() {
    return kryoOptions;
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    KryoAtomicCoderUtil.encode(kryo.get(), value, outStream, context);
  }

  @Override
  public T decode(InputStream inStream, Context context) throws CoderException, IOException {
    return KryoAtomicCoderUtil.decode(kryo.get(), inStream, context);
  }

  private Object writeReplace() {
    return new SerializableKryoCoderProxy<>(kryoOptions);
  }

  private static class SerializableKryoCoderProxy<T> implements Serializable {
    private final KryoOptions kryoOptions;

    public SerializableKryoCoderProxy(KryoOptions kryoOptions) {
      this.kryoOptions = kryoOptions;
    }

    private Object readResolve() {
      return new KryoAtomicCoder<T>(kryoOptions);
    }
  }

}
