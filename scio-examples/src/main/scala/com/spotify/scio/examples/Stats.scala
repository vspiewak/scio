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

package com.spotify.scio.examples

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.dataflow.model.Job
import com.google.api.services.dataflow.{Dataflow, DataflowScopes}

import scala.collection.JavaConverters._

object Stats {
  def main(args: Array[String]): Unit = {
    val df = new DataflowClient("gabocontinuousintegration")
    val jobs = df.listJobs()
    jobs.foreach { j =>
      println(j.toPrettyString)
      println()
    }

    println(jobs.head.toPrettyString)
    println()
    println(df.getJob(jobs.head.getId).toPrettyString)
    println(df.getJob(jobs.head.getId).getEnvironment.getUserAgent.get("name"))
    println(df.getJob(jobs.head.getId).getEnvironment.getUserAgent.get("version"))
  }
}

class DataflowClient(val projectId: String) {

  private val df = {
    val credential = GoogleCredential.getApplicationDefault.createScoped(DataflowScopes.all())
    new Dataflow.Builder(new NetHttpTransport, new JacksonFactory, credential)
      .setApplicationName("scio")
      .build()
  }

  def listJobs(): Seq[Job] = {
    var r = df.projects().jobs().list(projectId).execute()
    val jobs = r.getJobs.asScala
    while (r.getNextPageToken != null) {
      r = df.projects().jobs().list(projectId).setPageToken(r.getNextPageToken).execute()
      jobs ++= r.getJobs.asScala
    }
    jobs
  }

  def getJob(jobId: String): Job = df.projects().jobs().get(projectId, jobId).execute()

}
