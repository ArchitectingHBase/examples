package com.architecting.ch07;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class ReadFromHBase {

  public static final TableName sensorsTableName = TableName.valueOf("sensors");
  public static final Log LOG = LogFactory.getLog(ReadFromHBase.class);

  /**
   * Read the first column of the first row and display it's AVRO content.
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    Configuration config = HBaseConfiguration.create();
    // tag::SETUP[]
    try (Connection connection = ConnectionFactory.createConnection(config);
         Table sensorsTable = connection.getTable(sensorsTableName)) {  // <1>
      Scan scan = new Scan ();
      scan.setCaching(1); // <2>

      ResultScanner scanner = sensorsTable.getScanner(scan);
      Result result = scanner.next(); // <3>
      if (result != null && !result.isEmpty()) {
        Event event = new Util().cellToEvent(result.listCells().get(0), null); // <4>
        LOG.info("Retrived AVRO content: " + event.toString());
      } else {
        LOG.error("Impossible to find requested cell");
      }
    }
    // end::SETUP[]
  }
}
