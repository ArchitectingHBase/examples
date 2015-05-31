/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License");you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or
 * agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.architecting.ch09;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;

import com.google.common.base.Charsets;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.hadoop.SolrCloudPartitioner;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.apache.solr.hadoop.SolrReducer;
import org.apache.solr.hadoop.TreeMergeMapper;
import org.apache.solr.hadoop.Utils;
import org.apache.solr.hadoop.AlphaNumericComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;

/**
 * Public API for a MapReduce batch job driver that creates a set of Solr index shards from a set of
 * input files and writes the indexes into HDFS, in a flexible, scalable and fault-tolerant manner.
 * Also supports merging the output shards into a set of live customer facing Solr servers,
 * typically a SolrCloud.
 */
public class MapReduceIndexerTool extends Configured implements Tool {

  Job job;// visible for testing only

  public static final String RESULTS_DIR = "results";

  static final String MAIN_MEMORY_RANDOMIZATION_THRESHOLD = MapReduceIndexerTool.class.getName()
      + ".mainMemoryRandomizationThreshold";

  private static final String FULL_INPUT_LIST = "full-input-list.txt";

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceIndexerTool.class);

  static List<List<String>> buildShardUrls(List<Object> urls, Integer numShards) {
    if (urls == null) return null;
    List<List<String>> shardUrls = new ArrayList<List<String>>(urls.size());
    List<String> list = null;

    int sz;
    if (numShards == null) {
      numShards = urls.size();
    }
    sz = (int) Math.ceil(urls.size() / (float) numShards);
    for (int i = 0; i < urls.size(); i++) {
      if (i % sz == 0) {
        list = new ArrayList<String>();
        shardUrls.add(list);
      }
      list.add((String) urls.get(i));
    }

    return shardUrls;
  }

  static final class Options {
    boolean goLive;
    String collection;
    String zkHost;
    Integer goLiveThreads;
    List<List<String>> shardUrls;
    String inputTable;
    Path outputDir;
    int mappers;
    int reducers;
    int fanout;
    Integer shards;
    int maxSegments;
    File solrHomeDir;
    File log4jConfigFile;
  }

  // END OF INNER CLASS

  /** API for command line clients */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MapReduceIndexerTool(), args);
    System.exit(res);
  }

  public MapReduceIndexerTool() {
  }

  @Override
  public int run(String[] args) throws Exception {
    Options opts = new Options();
    opts.inputTable = "sensors";
    opts.outputDir = new Path("ch09/searchOutput");
    opts.mappers = -1;
    opts.reducers = -2;
    opts.fanout = Integer.MAX_VALUE;
    opts.maxSegments = 1;
    opts.solrHomeDir = new File("/home/cloudera/ahae/resources/ch09/search");
    opts.zkHost = "localhost:2181/solr";
    opts.shards = 1;
    opts.shardUrls = new ArrayList<List<String>>() {
      private static final long serialVersionUID = 7588910418917691909L;
      {
        add(new ArrayList<String>() {
          private static final long serialVersionUID = 7983627602491789899L;
          {
            add("http://quickstart.cloudera:8983/solr/Ch09-Collection_shard1_replica1/");
          }
        });
      }
    };
    opts.goLive = true;
    opts.goLiveThreads = 1000;
    opts.collection = "Ch09-Collection";

    return run(opts);
  }

  /** API for Java clients;visible for testing;may become a public API eventually */
  int run(Options options) throws Exception {
    if (getConf().getBoolean("isMR1", false) && "local".equals(getConf().get("mapred.job.tracker"))) {
      throw new IllegalStateException(
          "Running with LocalJobRunner (i.e. all of Hadoop inside a single JVM) is not supported "
              + "because LocalJobRunner does not (yet) implement the Hadoop Distributed Cache feature, "
              + "which is required for passing files via --files and --libjars");
    }

    long programStartTime = System.nanoTime();
    getConf().setInt(SolrOutputFormat.SOLR_RECORD_WRITER_MAX_SEGMENTS, options.maxSegments);

    // switch off a false warning about allegedly not implementing Tool
    // also see http://hadoop.6.n7.nabble.com/GenericOptionsParser-warning-td8103.html
    // also see https://issues.apache.org/jira/browse/HADOOP-8183
    getConf().setBoolean("mapred.used.genericoptionsparser", true);

    if (options.log4jConfigFile != null) {
      Utils.setLogConfigFile(options.log4jConfigFile, getConf());
      addDistributedCacheFile(options.log4jConfigFile, getConf());
    }

    Configuration config = HBaseConfiguration.create();
    job = Job.getInstance(config);
    job.setJarByClass(getClass());

    // To be able to run this example from eclipse, we need to make sure 
    // the built jar is distributed to the map-reduce tasks from the
    // local file system.
    job.addCacheArchive(new URI("file:///home/cloudera/ahae/target/architecting-hbase-applications-examples-1.jar"));

    int mappers = new JobClient(job.getConfiguration()).getClusterStatus().getMaxMapTasks();// MR1
    LOG.info("Cluster reports {} mapper slots", mappers);

    FileSystem fs = options.outputDir.getFileSystem(job.getConfiguration());
    if (fs.exists(options.outputDir) && !delete(options.outputDir, true, fs)) {
      return -1;
    }
    Path outputResultsDir = new Path(options.outputDir, RESULTS_DIR);
    Path outputReduceDir = new Path(options.outputDir, "reducers");

    int realMappers = 15;
    int reducers = 1;
    LOG.info(
      "Using these parameters: "
          + "Table: {}, mappers: {}, realMappers: {}, reducers: {}, shards: {}, fanout: {}, maxSegments: {}",
      new Object[] { 1, mappers, realMappers, reducers, options.shards, options.fanout,
          options.maxSegments });

    long startTime = System.currentTimeMillis();
    long secs = (System.currentTimeMillis() - startTime) / 1000;

    Scan scan = new Scan();
    scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
    scan.setCacheBlocks(false);  // don't set to true for MR jobs

    TableMapReduceUtil.initTableMapperJob(
      options.inputTable,              // input HBase table name
      scan,                            // Scan instance to control CF and attribute selection
      HBaseAvroToSOLRMapper.class,     // mapper
      Text.class,                      // mapper output key
      SolrInputDocumentWritable.class, // mapper output value
      job);

    FileOutputFormat.setOutputPath(job, outputReduceDir);

    job.setJobName(getClass().getName() + "/" + Utils.getShortClassName(HBaseAvroToSOLRMapper.class));
    job.setReducerClass(SolrReducer.class);
    job.setPartitionerClass(SolrCloudPartitioner.class);
    job.getConfiguration().set(SolrCloudPartitioner.ZKHOST, options.zkHost);
    job.getConfiguration().set(SolrCloudPartitioner.COLLECTION, options.collection);
    job.getConfiguration().setInt(SolrCloudPartitioner.SHARDS, options.shards);

    job.setOutputFormatClass(SolrOutputFormat.class);
    SolrOutputFormat.setupSolrHomeCache(options.solrHomeDir, job);


    job.setNumReduceTasks(reducers);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(SolrInputDocumentWritable.class);
    LOG.info("Indexing {} table using {} real mappers into {} reducers", new Object[] { 1,
        realMappers, reducers });
    startTime = System.nanoTime();
    if (!waitForCompletion(job, true)) {
      return -1;// job failed
    }

    // -------------------------------------------------------------------------------------------------------------------------------------

    secs = (System.currentTimeMillis() - startTime) / 1000;
    LOG.info("Done. Indexing {} table using {} real mappers into {} reducers took {} secs",
      new Object[] { 1, realMappers, reducers, secs });

    assert reducers == options.shards;

    // normalize output shard dir prefix, i.e.
    // rename part-r-00000 to part-00000 (stems from zero tree merge iterations)
    // rename part-m-00000 to part-00000 (stems from > 0 tree merge iterations)
    for (FileStatus stats : fs.listStatus(outputReduceDir)) {
      String dirPrefix = SolrOutputFormat.getOutputName(job);
      Path srcPath = stats.getPath();
      if (stats.isDirectory() && srcPath.getName().startsWith(dirPrefix)) {
        String dstName =
            dirPrefix + srcPath.getName().substring(dirPrefix.length() + "-m".length());
        Path dstPath = new Path(srcPath.getParent(), dstName);
        if (!rename(srcPath, dstPath, fs)) {
          return -1;
        }
      }
    }
    ;

    // publish results dir
    if (!rename(outputReduceDir, outputResultsDir, fs)) {
      return -1;
    }

    if (options.goLive
        && !new GoLive().goLive(options, listSortedOutputShardDirs(outputResultsDir, fs))) {
      return -1;
    }

    goodbye(job, programStartTime);
    return 0;
  }

  // do the same as if the user had typed 'hadoop ... --files <file>'
  private void addDistributedCacheFile(File file, Configuration conf) throws IOException {
    String HADOOP_TMP_FILES = "tmpfiles";// see Hadoop's GenericOptionsParser
    String tmpFiles = conf.get(HADOOP_TMP_FILES, "");
    if (tmpFiles.length() > 0) { // already present?
      tmpFiles = tmpFiles + ",";
    }
    GenericOptionsParser parser =
        new GenericOptionsParser(new Configuration(conf), new String[] { "--files",
            file.getCanonicalPath() });
    String additionalTmpFiles = parser.getConfiguration().get(HADOOP_TMP_FILES);
    assert additionalTmpFiles != null;
    assert additionalTmpFiles.length() > 0;
    tmpFiles += additionalTmpFiles;
    conf.set(HADOOP_TMP_FILES, tmpFiles);
  }

  private FileStatus[] listSortedOutputShardDirs(Path outputReduceDir, FileSystem fs)
      throws FileNotFoundException, IOException {
    final String dirPrefix = SolrOutputFormat.getOutputName(job);
    FileStatus[] dirs = fs.listStatus(outputReduceDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(dirPrefix);
      }
    });
    for (FileStatus dir : dirs) {
      if (!dir.isDirectory()) {
        throw new IllegalStateException("Not a directory: " + dir.getPath());
      }
    }

    // use alphanumeric sort (rather than lexicographical sort) to properly handle more than 99999
    // shards
    Arrays.sort(dirs, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus f1, FileStatus f2) {
        return new AlphaNumericComparator().compare(f1.getPath().getName(), f2.getPath().getName());
      }
    });

    return dirs;
  }

  /*
   * You can run MapReduceIndexerTool in Solrcloud mode, and once the MR job completes, you can use
   * the standard solrj Solrcloud API to send doc updates and deletes to SolrCloud, and those
   * updates and deletes will go to the right Solr shards, and it will work just fine. The MapReduce
   * framework doesn't guarantee that input split N goes to the map task with the taskId = N. The
   * job tracker and Yarn schedule and assign tasks, considering data locality aspects, but without
   * regard of the input split# withing the overall list of input splits. In other words, split# !=
   * taskId can be true. To deal with this issue, our mapper tasks write a little auxiliary metadata
   * file (per task) that tells the job driver which taskId processed which split#. Once the
   * mapper-only job is completed, the job driver renames the output dirs such that the dir name
   * contains the true solr shard id, based on these auxiliary files. This way each doc gets
   * assigned to the right Solr shard even with #reducers > #solrshards Example for a merge with two
   * shards: part-m-00000 and part-m-00001 goes to outputShardNum = 0 and will end up in merged
   * part-m-00000 part-m-00002 and part-m-00003 goes to outputShardNum = 1 and will end up in merged
   * part-m-00001 part-m-00004 and part-m-00005 goes to outputShardNum = 2 and will end up in merged
   * part-m-00002 ... and so on Also see run() method above where it uses
   * NLineInputFormat.setNumLinesPerSplit(job, options.fanout) Also see
   * TreeMergeOutputFormat.TreeMergeRecordWriter.writeShardNumberFile()
   */
  private boolean renameTreeMergeShardDirs(Path outputTreeMergeStep, Job job, FileSystem fs)
      throws IOException {
    final String dirPrefix = SolrOutputFormat.getOutputName(job);
    FileStatus[] dirs = fs.listStatus(outputTreeMergeStep, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(dirPrefix);
      }
    });

    for (FileStatus dir : dirs) {
      if (!dir.isDirectory()) {
        throw new IllegalStateException("Not a directory: " + dir.getPath());
      }
    }

    // Example: rename part-m-00004 to _part-m-00004
    for (FileStatus dir : dirs) {
      Path path = dir.getPath();
      Path renamedPath = new Path(path.getParent(), "_" + path.getName());
      if (!rename(path, renamedPath, fs)) {
        return false;
      }
    }

    // Example: rename _part-m-00004 to part-m-00002
    for (FileStatus dir : dirs) {
      Path path = dir.getPath();
      Path renamedPath = new Path(path.getParent(), "_" + path.getName());

      // read auxiliary metadata file (per task) that tells which taskId
      // processed which split# aka solrShard
      Path solrShardNumberFile = new Path(renamedPath, TreeMergeMapper.SOLR_SHARD_NUMBER);
      InputStream in = fs.open(solrShardNumberFile);
      byte[] bytes = ByteStreams.toByteArray(in);
      in.close();
      Preconditions.checkArgument(bytes.length > 0);
      int solrShard = Integer.parseInt(new String(bytes, Charsets.UTF_8));
      if (!delete(solrShardNumberFile, false, fs)) {
        return false;
      }

      // same as FileOutputFormat.NUMBER_FORMAT
      NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
      numberFormat.setMinimumIntegerDigits(5);
      numberFormat.setGroupingUsed(false);
      Path finalPath =
          new Path(renamedPath.getParent(), dirPrefix + "-m-" + numberFormat.format(solrShard));

      LOG.info("MTree merge renaming solr shard: " + solrShard + " from dir: " + dir.getPath()
          + " to dir: " + finalPath);
      if (!rename(renamedPath, finalPath, fs)) {
        return false;
      }
    }
    return true;
  }

  private boolean waitForCompletion(Job job, boolean isVerbose) throws IOException,
      InterruptedException, ClassNotFoundException {

    LOG.debug("Running job: " + getJobInfo(job));
    boolean success = job.waitForCompletion(isVerbose);
    if (!success) {
      LOG.error("Job failed! " + getJobInfo(job));
    }
    return success;
  }

  private void goodbye(Job job, long startTime) {
    float secs = (System.nanoTime() - startTime) / (float) (10 ^ 9);
    if (job != null) {
      LOG.info("Succeeded with job: " + getJobInfo(job));
    }
    LOG.info("Success. Done. Program took {} secs. Goodbye.", secs);
  }

  private String getJobInfo(Job job) {
    return "jobName: " + job.getJobName() + ", jobId: " + job.getJobID();
  }

  private boolean rename(Path src, Path dst, FileSystem fs) throws IOException {
    boolean success = fs.rename(src, dst);
    if (!success) {
      LOG.error("Cannot rename " + src + " to " + dst);
    }
    return success;
  }

  private boolean delete(Path path, boolean recursive, FileSystem fs) throws IOException {
    boolean success = fs.delete(path, recursive);
    if (!success) {
      LOG.error("Cannot delete " + path);
    }
    return success;
  }

  private int createTreeMergeInputDirList(Path outputReduceDir, FileSystem fs, Path fullInputList)
      throws FileNotFoundException, IOException {
    FileStatus[] dirs = listSortedOutputShardDirs(outputReduceDir, fs);
    int numFiles = 0;
    FSDataOutputStream out = fs.create(fullInputList);
    try {
      Writer writer = new BufferedWriter(new OutputStreamWriter(out, Charsets.UTF_8));
      for (FileStatus stat : dirs) {
        LOG.debug("Adding path {}", stat.getPath());
        Path dir = new Path(stat.getPath(), "data/index");
        if (!fs.isDirectory(dir)) {
          throw new IllegalStateException("Not a directory: " + dir);
        }
        writer.write(dir.toString() + "\n");
        numFiles++;
      }
      writer.close();
    } finally {
      out.close();
    }
    return numFiles;
  }
}
