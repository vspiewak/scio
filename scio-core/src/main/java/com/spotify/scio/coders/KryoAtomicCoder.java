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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static scala.collection.JavaConversions.asScalaBuffer;

public class KryoAtomicCoder<T> extends AtomicCoder<T> {

  private final List<String> registrars;
  private final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
    @Override
    protected Kryo initialValue() {
      return KryoAtomicCoderUtil.newKryo(asScalaBuffer(registrars));
    }
  };

  public static <T> KryoAtomicCoder<T> of() {
    return new KryoAtomicCoder<>(Collections.<String>emptyList());
  }

  @JsonCreator
  public static <T> KryoAtomicCoder<T> of(@JsonProperty("registrars") List<String> registrars) {
    return new KryoAtomicCoder<>(registrars);
  }

  protected KryoAtomicCoder(final List<String> registrars) {
    this.registrars = registrars;
  }

  public List<String> getRegistrars() {
    return registrars;
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
    return new SerializableKryoCoderProxy<>(registrars);
  }

  private static class SerializableKryoCoderProxy<T> implements Serializable {
    private final List<String> registrars;

    public SerializableKryoCoderProxy(List<String> registrars) {
      this.registrars = registrars;
    }

    private Object readResolve() {
      return new KryoAtomicCoder(registrars);
    }
  }

}
