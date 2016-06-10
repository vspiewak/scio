package com.google.cloud.dataflow.contrib.hadoop;

import com.google.cloud.dataflow.sdk.io.TextIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HadoopTextIO {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopTextIO.class);
  private static final long serialVersionUID = 0L;

  protected static final String dest = "gs://ravioli/dist_cp/staging";

  private HadoopTextIO() {}

  public static TextIO.Read.Bound<String> from(String pattern) throws Exception {
    Configuration conf = new Configuration();

    FileStatus[] statuses = FileSystem.get(conf).globStatus(new Path(pattern));
    List<Path> paths = new ArrayList<>(statuses.length);
    for(FileStatus status : statuses) {
      LOG.info("Will copy " + status.getPath().getName() + " to " + dest);
      paths.add(status.getPath());
    }

    DistCpOptions cpOptions = new DistCpOptions(paths, new Path(dest));

    DistCp distCp = new DistCp(conf, cpOptions);
    Job job = distCp.createAndSubmitJob();
    LOG.info("distcp created and submitted - waiting for finish");
    distCp.waitForJobCompletion(job);

    LOG.info("distcp job finished!");

    return TextIO.Read.from(pattern);
  }
}
