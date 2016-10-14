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

package com.spotify.mq.mm

import com.spotify.data.types.metadata.{Album, Artist}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection

object MasterMetadataCrazyCase {

  def main(cmdlineArgs: Array[String]): Unit = {

    val runId = cmdlineArgs(0)
    val argz = Array(
      "--runner=DataflowPipelineRunner",
      "--zone=europe-west1-d",
      "--project=master-metadata-pipeline",
      "--stagingLocation=gs://mastermetadata/staging/core/typed/MasterMetadata/2016-10-05",
      "--album=gs://mastermetadata/metadata/typed/dumps/2016-10-05/album/*.avro",
      "--artist=gs://mastermetadata/metadata/typed/dumps/2016-10-05/artist/*.avro",
      "--output=gs://mastermetadata/tmp/join-dbg/join-" + runId,
      "--numWorkers=10",
      "--workerMachineType=n1-standard-4"
    )
    val (sc, args) = ContextAndArgs(argz)

    val artists = sc.avroFile[Artist](args("artist"))
      .filter(_.getRedirect == null)
      .keyBy(_.getId)
    val albums = sc.avroFile[Album](args("album"))
      .filter(_.getRedirect == null)
      .keyBy(_.getArtistId)

//    val out: SCollection[Album] = albums.join(artists).map(_._2._1)
    val out: SCollection[Album] = artists.join(albums).map(_._2._2)
    out.saveAsAvroFile(args("output"))

    sc.close()
  }

}
