package com.architecting.ch07;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConvertToHFilesMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Cell> {

  public static final byte[] CF = Bytes.toBytes("v");
  // tag::VARIABLES[]
  public static final EncoderFactory encoderFactory = EncoderFactory.get();
  public static final ByteArrayOutputStream out = new ByteArrayOutputStream();
  public static final DatumWriter<Event> writer = new SpecificDatumWriter<Event>
                                                             (Event.getClassSchema());
  public static final BinaryEncoder encoder = encoderFactory.binaryEncoder(out, null);
  public static final Event event = new Event();
  public static final ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
  // end::VARIABLES[]



  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    super.setup(context);
    context.getCounter("Convert", "mapper").increment(1);
  }



  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    // tag::MAP[]
    // Extract the different fields from the received line.
    String[] line = value.toString().split(","); // <1>

    event.setId(line[0]);
    event.setEventId(line[1]);
    event.setDocType(line[2]);
    event.setPartName(line[3]);
    event.setPartNumber(line[4]);
    event.setVersion(Long.parseLong(line[5]));
    event.setPayload(line[6]);  // <2>

    // Serialize the AVRO object into a ByteArray
    out.reset(); // <3>
    writer.write(event, encoder); // <4>
    encoder.flush();

    byte[] rowKeyBytes = DigestUtils.md5(line[0]);
    rowKey.set(rowKeyBytes); // <5>
    context.getCounter("Convert", line[2]).increment(1);

    KeyValue kv = new KeyValue(rowKeyBytes, CF, Bytes.toBytes(line[1]), out.toByteArray()); // <6>
    context.write (rowKey, kv); // <7>
    // end::MAP[]
  }
}
