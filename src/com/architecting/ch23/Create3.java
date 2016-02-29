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
import org.apache.hadoop.hbase.util.RegionSplitter.UniformSplit;

public class Create3 {

  public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    // tag::CREATE3[]
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("crc"));
    desc.setMaxFileSize((long)20*1024*1024*1024);
    desc.setConfiguration("hbase.hstore.compaction.min", "5");
    HColumnDescriptor family = new HColumnDescriptor("c");
    family.setInMemory(true);
    desc.addFamily(family);
    UniformSplit uniformSplit = new UniformSplit();
    admin.createTable(desc, uniformSplit.split(64));
    // end::CREATE3[]
    admin.close();
  }
}
