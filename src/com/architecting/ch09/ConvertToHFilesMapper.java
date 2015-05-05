package com.architecting.ch09;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConvertToHFilesMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Cell> {

  public static final byte[] CF = Bytes.toBytes("v");

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    // Extract the different fields from the received line.
    String[] line = value.toString().split(",");

    // Tranfert them 
    Event event = Event.newBuilder()
                  .setId(line[0])
                  .setEventid(line[1])
                  .setDocType(line[2])
                  .setPartName(line[3])
                  .setPartNumber(line[4])
                  .setVersion(Long.parseLong(line[5]))
                  .setPayload(line[6])
                  .build();

    // Serialize the AVRO object into a BytArray
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    DatumWriter<Event> writer = new SpecificDatumWriter<Event>(Event.getClassSchema());
    writer.write(event, encoder);
    encoder.flush();
    out.close();

    ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(line[0]));
    KeyValue kv = new KeyValue(rowKey.get(), CF, Bytes.toBytes(line[1]), out.toByteArray());
    context.write (rowKey, kv);
  }
}
