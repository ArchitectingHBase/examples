package com.architecting.ch25;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

public class Create2 {

  public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    // tag::CREATE2[]
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("pages"));
    byte[][] splits = {Bytes.toBytes("b"), Bytes.toBytes("f"),
      Bytes.toBytes("k"), Bytes.toBytes("n"), Bytes.toBytes("t")};
    desc.setValue(Bytes.toBytes("comment"), Bytes.toBytes("Create 10012014"));
    HColumnDescriptor family = new HColumnDescriptor("c");
    family.setCompressionType(Algorithm.GZ);
    family.setMaxVersions(52);
    family.setBloomFilterType(BloomType.ROW);
    desc.addFamily(family);
    admin.createTable(desc, splits);
    // end::CREATE2[]
    admin.close();
  }
}
