package com.architecting.ch15;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class TestMOB {

  public static void main(String[] args) throws IOException {
    Configuration config = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(config);
        Table table = connection.getTable(TableName.valueOf("mob_test"))) {
      // tag::MOB[]
      byte[] rowKey = Bytes.toBytes("rowKey");
      byte[] CF = Bytes.toBytes("f");
      byte[] smallCellCQ = Bytes.toBytes("small");
      byte[] bigCellCQ = Bytes.toBytes("big");
      byte[] smallCellValue = new byte[1024];
      byte[] bigCellValue = new byte[110000];
      System.out.println ("Putting small value");
      Put smallPut = new Put(rowKey);
      smallPut.addColumn(CF, smallCellCQ, smallCellValue);
      table.put (smallPut);
      System.out.println ("Putting big value");
  
      Put bigPut = new Put(rowKey);
      bigPut.addColumn(CF, bigCellCQ, bigCellValue);
      table.put (bigPut);
      // end::MOB[]
    }
  }

}
