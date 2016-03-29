package com.architecting.ch07;

import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

public class Util {
  protected BinaryDecoder decoder = null;
  protected SpecificDatumReader<Event> reader = new SpecificDatumReader<Event>(Event.getClassSchema());

  /**
   * Convert a HBase cell into it's Event AVRO representation.
   * @param cell the HBase cell to convert.
   * @param reuse an existing Event object to reuse, if any. Else, null.
   * @return the AVRO representation of the HBae cell.
   * @throws IOException
   */
  public Event cellToEvent (Cell cell, Event reuse) throws IOException {
    decoder = DecoderFactory.get().binaryDecoder(Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()), decoder);
    reuse = reader.read(reuse, decoder);
    return reuse;
  }
}
