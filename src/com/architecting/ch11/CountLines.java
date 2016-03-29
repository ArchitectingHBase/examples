package com.architecting.ch11;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CountLines {

  @SuppressWarnings("serial")
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseBulkGetExample ").setMaster("local[2]");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    JavaRDD<String> textFile = jsc.textFile("hdfs://localhost/user/cloudera/data.txt");
    JavaPairRDD<String, Integer> pairs = textFile.mapToPair(new PairFunction<String, String, Integer>() {
      public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s.substring(0, s.indexOf("|")), 1); }
    });
    JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
      public Integer call(Integer a, Integer b) { return a + b; }
    });
    System.out.println ("We have generaged " + counts.count() + " users");
    jsc.close();
  }

}
