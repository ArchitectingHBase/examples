package com.architecting.ch09;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import static com.architecting.ch09.DocumentInterceptor.*;

public class DocumentSerializer implements HbaseEventSerializer {

  public static final Log LOG = LogFactory.getLog(DocumentSerializer.class);

  private byte[] payload;
  private byte[] cf;
  private BinaryDecoder decoder = null;
  private Document document = null;
  private Map<String, String> headers = null;
  private SpecificDatumReader<Document> reader = null;

  @Override
  public void configure(Context context) {
    LOG.info("Performing configuration from Context.");
  }

  @Override
  public void configure(ComponentConfiguration conf) {
    LOG.info("Performing configuration from ComponentConfiguration.");
  }

  // tag::SERIALIZE1[]
  @Override
  public void initialize(Event event, byte[] cf) {
    if (LOG.isDebugEnabled()) LOG.debug("Performing initialization for a "
        + event.getBody().length + " bytes event");
    else LOG.info("Performing initialization for an event");
    this.payload = event.getBody(); // <1>
    this.cf = cf;
    this.headers = event.getHeaders();
    reader = new SpecificDatumReader<Document>(Document.class);
  }

  @Override
  public List<Row> getActions() throws FlumeException {
    if (LOG.isInfoEnabled()) LOG.info("Retrieving actions.");
    List<Row> actions = new LinkedList<Row>();
    try {
      decoder = DecoderFactory.get().binaryDecoder(payload, decoder);
      document = reader.read(document, decoder); // <2>
      byte[] rowKeyBytes =
          Bytes.add(DigestUtils.md5("" + document.getSin()),
            Bytes.toBytes(document.getSin().intValue()));
      LOG.info("SIN = " + document.getSin());
      LOG.info("rowKey = " + Bytes.toStringBinary(rowKeyBytes));
      Put put = new Put(rowKeyBytes);
      put.addColumn(cf, Bytes.toBytes(System.currentTimeMillis()), payload); // <3>
      actions.add(put);
      String firstCellColumn;
      if ((firstCellColumn = headers.get(COLUMN)) != null) {
        String payload = headers.get(PAYLOAD);
        put.addColumn(cf, Base64.decode(firstCellColumn), Base64.decode(payload)); // <4>
        LOG.info("Updating first cell "
            + Bytes.toStringBinary(Base64.decode(firstCellColumn)));
      }
    } catch (Exception e) {
      LOG.error("Unable to serialize flume event to HBase action!", e);
      throw new FlumeException("Unable to serialize flume event to HBase action!",
          e);
    }
    return actions;
  }
  // end::SERIALIZE1[]

  @Override
  public List<Increment> getIncrements() {
    return new LinkedList<Increment>();
  }

  @Override
  public void close() {
  }
}
