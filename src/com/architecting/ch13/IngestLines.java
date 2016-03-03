package com.architecting.ch13;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorImpl;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class IngestLines {

  public static final TableName tableName = TableName.valueOf("user");

  @SuppressWarnings("serial")
  public static void main(String[] args) {
    long time1 = System.currentTimeMillis();
    SparkConf sparkConf = new SparkConf().setAppName("IngestLines ").setMaster("local[2]");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    Configuration conf = HBaseConfiguration.create();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    JavaRDD<String> textFile = jsc.textFile("hdfs://localhost/user/cloudera/data.txt");

    BufferedMutatorImpl foo = null;
    hbaseContext.bulkPut(textFile, tableName, new Function<String, Put>() {
      @Override
      public Put call(String v1) throws Exception {
        String[] tokens = v1.split("\\|");
        Put put = new Put(Bytes.toBytes(tokens[0]));
        // System.out.println ("Pas normal " + v1 + " => " + tokens[0] + ";" + tokens[1] + ";" + tokens[2]);
        put.addColumn(Bytes.toBytes("segment"), Bytes.toBytes(tokens[1]), Bytes.toBytes(tokens[2]));
        return put;
      }
    });
    jsc.close();
    long time2 = System.currentTimeMillis();
    System.out.println ("Took " + (time2 - time1) + " milliseconds");
  }

}
