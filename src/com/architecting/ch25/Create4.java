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

public class Create4 {

  public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    // tag::CREATE4[]
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("access"));
    HColumnDescriptor family = new HColumnDescriptor("d");
    family.setValue("comment", "Last user access date");
    family.setMaxVersions(10);
    family.setMinVersions(2);
    family.setTimeToLive(2678400);
    desc.addFamily(family);
    admin.createTable(desc);
    // end::CREATE4[]
    admin.close();
  }

}
