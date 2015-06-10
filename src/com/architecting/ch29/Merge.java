package com.architecting.ch29;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;


public class Merge {

  private static final Log LOG = LogFactory.getLog(Merge.class);

  public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    // tag::MERGE1[]
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
    List<HRegionInfo> regions = admin.getTableRegions(TableName.valueOf("t1")); //<1>
    LOG.info("testtable contains " + regions.size() + " regions.");
    for (int index = 0; index < regions.size() / 2; index++) {
      HRegionInfo region1 = regions.get(index*2);
      HRegionInfo region2 = regions.get(index*2+1);
      LOG.info("Merging regions " + region1 + " and " + region2);
      admin.mergeRegions(region1.getEncodedNameAsBytes(), 
                         region2.getEncodedNameAsBytes(), false); //<2>
    }
    admin.close();
    // end::MERGE1[]
  }

}
