package com.architecting.ch13

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._

object SparkBulkLoad {
  def main(args: Array[String]) {

    val columnFamily1 = "segment"
    val stagingFolder = "/tmp/user"
    val tableNameString = "user"
    val tableName = TableName.valueOf(tableNameString)

    val sc = new SparkContext(new SparkConf().setAppName("Spark BulkLoad").setMaster("local[4]"))
    val config = HBaseConfiguration.create
    val hbaseContext = new HBaseContext(sc, config)

    val textFile = sc.textFile("hdfs://localhost/user/cloudera/data.txt")
    val toByteArrays = textFile.map(line => {
      val tokens = line.split("\\|")
      (Bytes.toBytes(tokens(0)), (Bytes.toBytes(columnFamily1),
                                  Bytes.toBytes(tokens(1)),
                                  Bytes.toBytes(tokens(2))))
    })

    toByteArrays.hbaseBulkLoad(hbaseContext, tableName,
      t => {
        val rowKey = t._1
        val family:Array[Byte] = t._2._1
        val qualifier = t._2._2
        val value = t._2._3

        val keyFamilyQualifier= new KeyFamilyQualifier(rowKey, family, qualifier)

        Seq((keyFamilyQualifier, value)).iterator
      },
      stagingFolder)

    val load = new LoadIncrementalHFiles(config)
    load.run(Array(stagingFolder, tableNameString))
  }
}
