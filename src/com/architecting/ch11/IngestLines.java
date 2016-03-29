package com.architecting.ch11;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
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

  public static final TableName TABLE_NAME = TableName.valueOf("user");
  public static final byte[] COLUMN_FAMILY = Bytes.toBytes("segment");

  @SuppressWarnings("serial")
  public static void processVersion1() {
    long time1 = System.currentTimeMillis();
    // tag::BULKPUT[]
SparkConf sparkConf = new SparkConf().setAppName("IngestLines ")
                                     .setMaster("local[2]");
JavaSparkContext jsc = new JavaSparkContext(sparkConf);
Configuration conf = HBaseConfiguration.create();
JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
JavaRDD<String> textFile = 
                   jsc.textFile("hdfs://localhost/user/cloudera/data.txt");

hbaseContext.bulkPut(textFile, TABLE_NAME, new Function<String, Put>() {
  @Override
  public Put call(String v1) throws Exception {
    String[] tokens = v1.split("\\|");
    Put put = new Put(Bytes.toBytes(tokens[0]));
    put.addColumn(Bytes.toBytes("segment"),
                  Bytes.toBytes(tokens[1]),
                  Bytes.toBytes(tokens[2]));
    return put;
  }
});
jsc.close();
    // end::BULKPUT[]
    long time2 = System.currentTimeMillis();
    System.out.println("Took " + (time2 - time1) + " milliseconds");
  }

  @SuppressWarnings("serial")
  public static void processVersion2() {
long time1 = System.currentTimeMillis();
// tag::DETAILED[]
SparkConf sparkConf = new SparkConf().setAppName("IngestLines ")
                                      .setMaster("local[2]");
JavaSparkContext jsc = new JavaSparkContext(sparkConf);
Configuration conf = HBaseConfiguration.create();
JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
JavaRDD<String> textFile = 
                   jsc.textFile("hdfs://localhost/user/cloudera/data.txt");

PairFunction<String, String, String> linesplit = // <1>
new PairFunction<String, String, String>() {
  public Tuple2<String, String> call(String s) {
    int index = s.indexOf("|");
    return new Tuple2<String, String>(s.substring(0, index),
                                      s.substring(index + 1));
  }
};

JavaPairRDD<String, String> pairs = textFile.mapToPair(linesplit);
Function<String, List<String>> createCombiner =
new Function<String, List<String>>() {
  public List<String> call(String s) {
    List<String> list = new ArrayList<String>();
    list.add(s);
    return list;
  }
};

Function2<List<String>, String, List<String>> mergeValue =
new Function2<List<String>, String, List<String>>() {
  @Override
  public List<String> call(List<String> v1, String v2) throws Exception {
    v1.add(v2);
    return v1;
  }
};

Function2<List<String>, List<String>, List<String>> mergeCombiners =
new Function2<List<String>, List<String>, List<String>>() {
  @Override
  public List<String> call(List<String> v1, List<String> v2) throws Exception {
    v2.addAll(v1);
    return v2;
  }
};

JavaPairRDD<String, List<String>> keyValues = // <2>
            pairs.combineByKey(createCombiner, mergeValue, mergeCombiners);

JavaRDD<Put> keyValuesPuts = keyValues.map( // <3>
new Function<Tuple2<String, List<String>>, Put>() {
  @Override
  public Put call(Tuple2<String, List<String>> v1) throws Exception {
    Put put = new Put(Bytes.toBytes(v1._1));
    ListIterator<String> iterator = v1._2.listIterator();
    while (iterator.hasNext()) {
      String colAndVal = iterator.next();
      int indexDelimiter = colAndVal.indexOf("|");
      String columnQualifier = colAndVal.substring(0, indexDelimiter);
      String value = colAndVal.substring(indexDelimiter + 1);
      put.addColumn(COLUMN_FAMILY, Bytes.toBytes(columnQualifier),
                                   Bytes.toBytes(value));
    }
    return put;
  }
});

hbaseContext.foreachPartition(keyValuesPuts, // <4>
  new VoidFunction<Tuple2<Iterator<Put>, Connection>>() {
    @Override
    public void call(Tuple2<Iterator<Put>, Connection> t) throws Exception {
      Table table = t._2().getTable(TABLE_NAME);
      BufferedMutator mutator = t._2().getBufferedMutator(TABLE_NAME);
      while (t._1().hasNext()) {
        Put put = t._1().next();
        mutator.mutate(put);
      }

      mutator.flush();
      mutator.close();
      table.close();
    }
  });
jsc.close();
// end::DETAILED[]
long time2 = System.currentTimeMillis();
System.out.println(" Took " + (time2 - time1) + " milliseconds");
  }

  public static void main(String[] args) {
    processVersion2();
    processVersion1();
  }

}
