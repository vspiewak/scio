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

package com.spotify.scio.bigquery.types

import org.scalatest.{FlatSpec, Matchers}

class BigQueryOptionTest extends FlatSpec with Matchers {

  @BigQueryOption.flattenResults(value = true)
  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A1

  "BigQueryOption" should "support flattenResults(value = true)" in {
    A1.options.flattenResults shouldBe true
  }

  @BigQueryOption.flattenResults(value = false)
  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A2

  it should "support flattenResults(value = false)" in {
    A2.options.flattenResults shouldBe false
  }

  @BigQueryOption.flattenResults(true)
  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A3

  it should "support flattenResults(true)" in {
    A3.options.flattenResults shouldBe true
  }

  @BigQueryOption.flattenResults(false)
  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A4

  it should "support flattenResults(false)" in {
    A4.options.flattenResults shouldBe false
  }

  @BigQueryOption.flattenResults()
  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A5

  it should "support flattenResults()" in {
    A5.options.flattenResults shouldBe true
  }

  @BigQueryOption.flattenResults
  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A6

  it should "support flattenResults" in {
    A6.options.flattenResults shouldBe true
  }

  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class A7

  it should "have default flattenResults" in {
    A7.options.flattenResults shouldBe false
  }

}
