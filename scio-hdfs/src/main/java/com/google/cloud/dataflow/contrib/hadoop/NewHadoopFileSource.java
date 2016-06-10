package com.google.cloud.dataflow.contrib.hadoop;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NewHadoopFileSource<T> extends BoundedSource<T> {
  private static final Logger LOG = LoggerFactory.getLogger(NewHadoopFileSource.class);
  private static final long serialVersionUID = 0L;

  protected String filepattern;
  protected final String dest = "gs://ravioli/dist_cp/staging";

  @Override
  public List<? extends BoundedSource<T>> splitIntoBundles(long desiredBundleSizeBytes,
                                                          PipelineOptions options) throws Exception {

    return null;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return 0;
  }

  @Override
  public boolean producesSortedKeys(PipelineOptions options) throws Exception {
    return false;
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    assert(filepattern != null);

    Configuration conf = new Configuration();

    FileStatus[] statuses = FileSystem.get(conf).globStatus(new Path(filepattern));
    List<Path> paths = new ArrayList<>(statuses.length);
    for(FileStatus status : statuses) {
      paths.add(status.getPath());
    }

    DistCpOptions cpOptions = new DistCpOptions(paths, new Path(dest));

    try {
      DistCp distCp = new DistCp(conf, cpOptions);
      Job job = distCp.execute();
      distCp.waitForJobCompletion(job);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public void validate() {
    //TODO: nothing here - yet
  }

  @Override
  public Coder<T> getDefaultOutputCoder() {
    return null;
  }
}
