package com.architecting.ch07;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

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
  public static Event event = null;
  public static final Log LOG = LogFactory.getLog(HBaseAvroToSOLRMapper.class);
  public Util util = new Util(); // Get a ThreadSafe util class.
  protected SolrInputDocument inputDocument = new SolrInputDocument();


  @Override
  public void map(ImmutableBytesWritable row, Result values, Context context)
      throws InterruptedException, IOException {

    context.getCounter("HBaseAvro", "Total size").increment(values.size());
    context.getCounter("HBaseAvro", "Uniq size").increment(1);
    for (Cell cell: values.listCells()) {
      try {
        // tag::SETUP[]
        event = util.cellToEvent(cell, event); // <1>

        inputDocument.clear(); // <2>
        inputDocument.addField("id", UUID.randomUUID().toString()); // <3> 
        inputDocument.addField("rowkey", row.get());
        inputDocument.addField("eventId", event.getEventId().toString());
        inputDocument.addField("docType", event.getDocType().toString());
        inputDocument.addField("partName", event.getPartName().toString());
        inputDocument.addField("partNumber", event.getPartNumber().toString());
        inputDocument.addField("version", event.getVersion());
        inputDocument.addField("payload", event.getPayload().toString());

        context.write(new Text(cell.getRowArray()),
                          new SolrInputDocumentWritable(inputDocument)); // <4>
        // end::SETUP[]
        context.getCounter("HBaseAvro", "passed").increment(1);
      } catch (Exception e) {
        context.getCounter("HBaseAvro", "failed").increment(1);
        LOG.error("Issue for "  + Bytes.toStringBinary(cell.getValueArray()), e);
      }
    }
  }


  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    // TODO Auto-generated method stub
    super.setup(context);
    LOG.info("Processing " + context.getInputSplit().toString());
    LOG.info("Processing " + Arrays.toString(context.getInputSplit().getLocations()));
  }
}