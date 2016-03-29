package com.architecting.ch13;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class TestMOB {
  public static final int KB = 1024;
  public static final int MB = KB * 1024;
  public static final byte[] columnFamily = Bytes.toBytes("f");
  public static final byte[] columnQualifierOnePiece = Bytes.toBytes(0);
  
  // tag::SPLIT[]
  public static void putBigCell (Table table, byte[] key, byte[] value) throws IOException {
    if (value.length < 10 * MB) {
      // The value is small enough to be handle as a single HBase cell.
      // There is no need to split it in pieces.
      Put put = new Put(key).addColumn(columnFamily, columnQualifierOnePiece, value);
      table.put(put);
    } else {
      byte[] buffer = new byte[10*MB];
      int index = 0;
      int piece = 1;
      Put put = new Put(key);
      while (index < value.length) {
        int length = Math.min((value.length - index), buffer.length);
        byte[] columnQualifier = Bytes.toBytes(piece);
        System.arraycopy(value, index, buffer, 0, length); // <1>
        KeyValue kv = new KeyValue(key, 0, key.length, // <2> 
                                   columnFamily, 0, columnFamily.length, 
                                   columnQualifier, 0, columnQualifier.length,
                                   put.getTimeStamp(), KeyValue.Type.Put,
                                   buffer, 0, length);
        put.add(kv);
        index += length + 1;
        piece ++;
      }
      table.put(put);
    } 
  }
  // end::SPLIT[]

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
      putBigCell(table, rowKey, new byte[50*MB]);
    }
  }

}
