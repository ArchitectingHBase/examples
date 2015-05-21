package com.architecting.ch09;

import java.io.IOException;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadFromHBase {

  public static final TableName sensorsTableName = TableName.valueOf("sensors");
  public static final byte[] rowid = Bytes.toBytes("000a");
  public static final byte[] family = Bytes.toBytes("v");
  public static final byte[] qualifier = Bytes.toBytes("f92acb5b-079a-42bc-913a-657f270a3dc1");
  public static final Log LOG = LogFactory.getLog(ReadFromHBase.class);

  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    Configuration config = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(config);
         Admin admin = connection.getAdmin()) {
      // tag::SETUP[]
      Table sensorsTable = connection.getTable(sensorsTableName); // <1>
      Get get = new Get(rowid).addColumn(family, qualifier); // <2>
      Result result = sensorsTable.get(get);  // <3>
      if (!result.isEmpty()) { // <4>
        SpecificDatumReader<Event> reader = new SpecificDatumReader<Event>(Event.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(result.value(), null);
        Event event = reader.read(null, decoder);  // <5>
        LOG.info("Retrieved AVRO object with ID " + event.getId());
        LOG.info("AVRO content: " + event.toString());
      } else {
        LOG.error("Impossible to find requested cell");
      }
      // end::SETUP[]
    }
  }
}
