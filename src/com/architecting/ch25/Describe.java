package com.architecting.ch25;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class Describe {

  public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    // Instantiate default HBase configuration object.
    // Configuration file must be in the classpath
    Configuration conf = HBaseConfiguration.create();
    // tag::DESCRIBE
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor desc = admin.getTableDescriptor(TableName.valueOf("crc"));
    Collection<HColumnDescriptor> families = desc.getFamilies();
    System.out.println("Table " + desc.getTableName() + " has " + families.size() + " family(ies)");
    for (Iterator<HColumnDescriptor> iterator = families.iterator(); iterator.hasNext();) {
      HColumnDescriptor family = iterator.next();
      System.out.println("Family details: " + family);
    }
    // end::DESCRIBE
    admin.close();
  }
}
