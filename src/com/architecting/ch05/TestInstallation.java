package com.architecting.ch05;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.protobuf.ServiceException;
//tag::TEST1[]

public class TestInstallation {
  private static final Log LOG = LogFactory.getLog(TestInstallation.class);

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();
    try {
      LOG.info("Testing HBase connection...");
      HBaseAdmin.checkHBaseAvailable(conf);
      LOG.info("HBase is running correctly...");
    } catch (MasterNotRunningException e) {
      LOG.error("Unable to find a running HBase instance", e);
    } catch (ZooKeeperConnectionException e) {
      LOG.error("Unable to connect to ZooKeeper", e);
    } catch (ServiceException e) {
      LOG.error("HBase service unavailable", e);
    } catch (IOException e) {
      LOG.error("Error when trying to get HBase status", e);
    }
  }
}
//end::TEST1[]
