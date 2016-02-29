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

public class Create1 {

  public static void main(String[] args)
    throws MasterNotRunningException, ZooKeeperConnectionException,
           IOException {
    // tag::CREATE1[]
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor desc = 
      new HTableDescriptor(TableName.valueOf("testtable_create1"));
    HColumnDescriptor family = new HColumnDescriptor("f1");
    desc.addFamily(family);
    admin.createTable(desc);
    // end::CREATE1[]
    admin.close();
  }
}
