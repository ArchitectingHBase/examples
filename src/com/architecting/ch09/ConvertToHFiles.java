package com.architecting.ch09;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
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
    System.out.println ("new");
    try {
      Configuration conf = HBaseConfiguration.create();
      Connection connection = ConnectionFactory.createConnection(conf);
      conf.set("mapreduce.jobtracker.address", "local");
      conf.set("fs.defaultFS","file:///");

      String inputPath = args[0];
      inputPath = "/home/jmspaggiari/workspace/architecting-hbase-applications-examples/resources/ch09/omneo.csv";
      String outputPath = args[1];
      outputPath = "/home/jmspaggiari/workspace/architecting-hbase-applications-examples/resources/ch09/output" + System.currentTimeMillis();
      final TableName tableName = TableName.valueOf(args[2]);
      Table table = connection.getTable(tableName);

      Job job = Job.getInstance(conf, "Convert CVS files to HFiles");
      //HFileOutputFormat2.configureIncrementalLoadMap(job, table);
      HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(tableName));

      job.setSpeculativeExecution(false);
      job.setReduceSpeculativeExecution(false);

      job.setInputFormatClass(TextInputFormat.class);

      job.setJarByClass(ConvertToHFiles.class);

      //job.setReducerClass(ConvertToHFilesReducer.class);
      job.setMapperClass(ConvertToHFilesMapper.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(KeyValue.class);

      FileInputFormat.setInputPaths(job, inputPath);
      HFileOutputFormat2.setOutputPath(job, new Path(outputPath));

      System.out.println(table.getTableDescriptor().getColumnFamilies()[0].getNameAsString());

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

  @Override
  public Configuration getConf() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setConf(Configuration arg0) {
    // TODO Auto-generated method stub
  }
}
