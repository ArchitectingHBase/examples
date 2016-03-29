package com.architecting.ch09;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.RegionSplitter.UniformSplit;

public class CreateTable {
  
  public static final Log LOG = LogFactory.getLog(CreateTable.class);

  public static void main(String[] args) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    try (Connection connection = ConnectionFactory.createConnection();
        Admin admin = connection.getAdmin();) {
      LOG.info("Starting table creation");
      // tag::CREATE[]
      TableName documents = TableName.valueOf("documents");
      HTableDescriptor desc = new HTableDescriptor(documents);
      HColumnDescriptor family = new HColumnDescriptor("c");
      family.setCompressionType(Algorithm.GZ);
      family.setBloomFilterType(BloomType.NONE);
      desc.addFamily(family);
      UniformSplit uniformSplit = new UniformSplit();
      admin.createTable(desc, uniformSplit.split(8));
      // end::CREATE[]
      LOG.info("Table successfuly created");
    }
  }

}
