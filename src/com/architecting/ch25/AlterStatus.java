package com.architecting.ch25;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class AlterStatus {

  public static void main(String[] args) {
    Configuration config = HBaseConfiguration.create();
    HBaseAdmin admin = null;
    try {
      // Instantiate HBase admin object using the configuration file.
      admin = new HBaseAdmin(config);

      long timeBefore = System.currentTimeMillis();
      // Loop until there is no more region to alter.
      while (admin.getAlterStatus(TableName.valueOf("testtable")).getFirst() > 0) {
        System.out.println("Still " + admin.getAlterStatus(TableName.valueOf("testtable")).getFirst() + " regions out of "
            + admin.getAlterStatus(TableName.valueOf("testtable")).getSecond() + " to upate.");
        Thread.sleep(1000);
      }
      System.out.println("Took " + ((System.currentTimeMillis() - timeBefore) / 1000) + " seconds to complet the alterations");
    } catch (IOException e) {
      System.out.println("Error while waiting for alter operations");
      e.printStackTrace();
    } catch (InterruptedException e) {
      System.out.println("Error while waiting for alter operations");
      e.printStackTrace();
    } finally {
      if (admin != null) try {
        admin.close();
      } catch (IOException e) {
        System.out.println("Error while releasing HBaseAdmin");
        e.printStackTrace();
      }
    }
  }
}
