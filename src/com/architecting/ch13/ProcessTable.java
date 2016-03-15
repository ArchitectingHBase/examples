package com.architecting.ch13;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.spark.HBaseContext;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class ProcessTable {

  public static final TableName TABLE_NAME = TableName.valueOf("user");
  public static final byte[] COLUMN_FAMILY = Bytes.toBytes("segment");

  @SuppressWarnings("serial")
  public static void processVersion1() {
    // tag::BULKPUT[]
    SparkConf sc = new SparkConf().setAppName("IngestLines ").setMaster("local[2]");
    JavaSparkContext jsc = new JavaSparkContext(sc);
    Configuration conf = HBaseConfiguration.create();

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
     
    Scan scan = new Scan();
    scan.setCaching(100);
    KeyOnlyFilter kof = new KeyOnlyFilter();
    scan.setFilter(kof);
         
    JavaRDD<Tuple2<ImmutableBytesWritable, Result>> data = hbaseContext.hbaseRDD(TableName.valueOf("user"), scan);
    FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>, Integer> setup = new FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>, Integer>() {
      @Override
      public Iterable<Integer> call(Iterator<Tuple2<ImmutableBytesWritable, Result>> input) {
        int a = 0;
        while (input.hasNext()) {
          a++;
          input.next();
        }
        ArrayList<Integer> ret = new ArrayList<Integer>();
        ret.add(a);
        return ret;
      }
    };
    Function2<Integer, Integer, Integer> combine = new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) {
        return a+b;
      }
    };
    
    Function2<Integer, Tuple2<ImmutableBytesWritable, Result>, Integer> aggregator = new Function2<Integer, Tuple2<ImmutableBytesWritable, Result>, Integer>() {
      @Override
      public Integer call(Integer v1, Tuple2<ImmutableBytesWritable, Result> v2) throws Exception {
        // TODO Auto-generated method stub
        return v1 + 1;
      }
    };
    Function2<Integer, Integer, Integer> combiner = new Function2<Integer, Integer, Integer>() {

      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        // TODO Auto-generated method stub
        return v1 + v2;
      }

    };
    
    long time1 = System.currentTimeMillis();
    System.out.println("data.count() = " + data.count());
    long time2 = System.currentTimeMillis();
    System.out.println("Took " + (time2 - time1) + " milliseconds");
    System.out.println("data.aggregate(0, aggregator, combiner) = " + data.aggregate(0, aggregator, combiner));
    long time3 = System.currentTimeMillis();
    System.out.println("Took " + (time3 - time2) + " milliseconds");
    System.out.println("data.mapPartitions(setup).reduce(combine) = " + data.mapPartitions(setup).reduce(combine));
    long time4 = System.currentTimeMillis();
    System.out.println("Took " + (time4 - time2) + " milliseconds");
    
    jsc.close();
    // end::BULKPUT[]
  }

  public static void main(String[] args) {
    processVersion1();
  }

}
