package com.architecting.ch09;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ConvertToHFiles extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(ConvertToHFiles.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ConvertToHFiles(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    try {
      Configuration conf = HBaseConfiguration.create();
      Connection connection = ConnectionFactory.createConnection(conf);

      String inputPath = args[0];
      String outputPath = args[1];
      final TableName tableName = TableName.valueOf(args[2]);

      // tag::SETUP[]
      Table table = connection.getTable(tableName);

      Job job = Job.getInstance(conf, "ConvertToHFiles: Convert CSV to HFiles");

      HFileOutputFormat2.configureIncrementalLoad(job, table,
                                        connection.getRegionLocator(tableName)); // <1>
      job.setInputFormatClass(TextInputFormat.class); // <2>

      job.setSpeculativeExecution(false); // <3>
      job.setReduceSpeculativeExecution(false); // <3>

      job.setJarByClass(ConvertToHFiles.class); // <4>
      job.setJar("/home/cloudera/ahae/target/ahae.jar"); // <4>

      job.setMapperClass(ConvertToHFilesMapper.class); // <5>
      job.setMapOutputKeyClass(ImmutableBytesWritable.class); // <6>
      job.setMapOutputValueClass(KeyValue.class); // <7>

      FileInputFormat.setInputPaths(job, inputPath);
      HFileOutputFormat2.setOutputPath(job, new Path(outputPath));
      // end::SETUP[]

      if (!job.waitForCompletion(true)) {
        LOG.error("Failure");
      } else {
        LOG.info("Success");
        return 0;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 1;
  }
}
