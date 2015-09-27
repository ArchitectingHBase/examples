package com.architecting.ch11;

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

public class CreateTable {
  
  public static final Log LOG = LogFactory.getLog(CreateTable.class);

  public static void main(String[] args) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    try (Connection connection = ConnectionFactory.createConnection();
        Admin admin = connection.getAdmin();) {
      // tag::CREATE[]
      HTableDescriptor desc =
                          new HTableDescriptor(TableName.valueOf("documents"));
      HColumnDescriptor family = new HColumnDescriptor("c");
      family.setCompressionType(Algorithm.GZ);
      family.setBloomFilterType(BloomType.NONE);
      desc.addFamily(family);
      admin.createTable(desc);
      // end::CREATE[]
      LOG.info("Table successfuly created");
    }
  }

}
