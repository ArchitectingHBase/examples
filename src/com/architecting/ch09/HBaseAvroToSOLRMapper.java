package com.architecting.ch09;

import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.SolrInputDocumentWritable;


public class HBaseAvroToSOLRMapper extends TableMapper<Text, SolrInputDocumentWritable> {
  public static final byte[] family = Bytes.toBytes("v");
  public static BinaryDecoder decoder = null;
  public static Event event = null;
  public static final Log LOG = LogFactory.getLog(HBaseAvroToSOLRMapper.class);
  public static boolean exit = false;

  /**
   * Avro returns Utf8 instances for the Strings. We need to convert
   * them to String instances for clean output.
   * @param sequence
   * @return
   */
  public static final String clean(CharSequence sequence, Context context) {
    /*
    if (sequence instanceof Utf8) {
      Utf8 utf8 = (Utf8)sequence;
      context.getCounter("HBaseAvro", "cleaned").increment(1);
      return new String((utf8).getBytes()).substring(0, utf8.getByteLength());
    }
    */
    return sequence.toString();
  }

  @Override
  public void map(ImmutableBytesWritable row, Result value, Context context)
      throws InterruptedException, IOException {
    for (Cell cell : value.rawCells()) {
      try {
        SpecificDatumReader<Event> reader = new SpecificDatumReader<Event>(Event.getClassSchema());
        decoder = DecoderFactory.get().binaryDecoder(Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()), decoder);
        event = reader.read(event, decoder);
        SolrInputDocument inputDocument = new SolrInputDocument();

        inputDocument.addField("solrkey", event.getId().toString() + event.getEventId().toString());
        inputDocument.addField("key", Bytes.toString(row.get()));
        inputDocument.addField("id", clean(event.getId(), context));
        inputDocument.addField("eventId", clean(event.getEventId(), context));
        inputDocument.addField("docType", clean(event.getDocType(), context));
        inputDocument.addField("partName", clean(event.getPartName(), context));
        inputDocument.addField("partNumber", clean(event.getPartNumber(), context));
        inputDocument.addField("version", event.getVersion());
        inputDocument.addField("payload", clean(event.getPayload(), context));

        context.write(new Text(cell.getRowArray()), new SolrInputDocumentWritable(inputDocument));
        context.getCounter("HBaseAvro", "passed").increment(1);
        context.getCounter("HBaseAvro", clean(event.getDocType(), context)).increment(1);
      } catch (Exception e) {
        context.getCounter("HBaseAvro", "failed").increment(1);
        LOG.error("Issue for "  + Bytes.toStringBinary(cell.getValueArray()), e);
      }
    }
  }
}