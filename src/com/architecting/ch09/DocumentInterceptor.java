package com.architecting.ch09;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.interceptor.Interceptor;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class DocumentInterceptor implements Interceptor {

  protected final XPath xpath = XPathFactory.newInstance().newXPath();
  protected Document document = new Document();
  protected Connection connection = null;
  public final EncoderFactory encoderFactory = EncoderFactory.get();
  public final ByteArrayOutputStream out = new ByteArrayOutputStream();
  public final DatumWriter<Document> writer = new SpecificDatumWriter<Document>(
      Document.getClassSchema());
  public final BinaryEncoder encoder = encoderFactory.binaryEncoder(out, null);
  public static final Log LOG = LogFactory.getLog(DocumentInterceptor.class);
  public static final TableName tableName = TableName.valueOf("documents");
  protected BinaryDecoder decoder = null;
  protected SpecificDatumReader<Document> reader =
      new SpecificDatumReader<Document>(Document.getClassSchema());

  public static final String COLUMN = "column";
  public static final String PAYLOAD = "payload";

  // public final StringBuffer eventString = new StringBuffer(128);

  private DocumentInterceptor(Context ctx) {
  }

  @Override
  public void initialize() {
    try {
      connection = ConnectionFactory.createConnection();
    } catch (IOException e) {
      LOG.error(e);
      throw new FlumeException(e);
    }
  }

  protected static NodeList getNodes(XPath xpath, String expression,
      InputSource inputSource) throws XPathExpressionException {
    return (NodeList) xpath.evaluate(expression, inputSource,
      XPathConstants.NODESET);
  }

  protected Document cellToAvro(Cell cell, Document reuse) throws IOException {
    byte[] value =
        Bytes.copy(cell.getValueArray(), cell.getValueOffset(),
          cell.getValueLength());
    decoder = DecoderFactory.get().binaryDecoder(value, decoder);
    reuse = reader.read(reuse, decoder);
    return reuse;
  }

  protected byte[] avroToBytes(Document document) throws IOException {
    out.reset();
    writer.write(document, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  @Override
  public Event intercept(final Event event) {
    String expression;
    NodeList nodes;
    String firstName = "";
    String lastName = "";
    long SIN = -1;
    String comment = "";

    ByteArrayInputStream inputAsInputStream =
        new ByteArrayInputStream(event.getBody());

    InputSource inputSource = new InputSource(inputAsInputStream);

    // initialize previous instantiated document
    try {
      // tag::INTERCEPT1[]
      expression = "/ClinicalDocument/PatientRecord/FirstName";
      nodes = getNodes(xpath, expression, inputSource);
      if (nodes.getLength() > 0) firstName = nodes.item(0).getTextContent();

      expression = "/ClinicalDocument/PatientRecord/LastName";
      inputAsInputStream.reset();
      nodes = getNodes(xpath, expression, inputSource);
      if (nodes.getLength() > 0) lastName = nodes.item(0).getTextContent();

      expression = "/ClinicalDocument/PatientRecord/SIN";
      inputAsInputStream.reset();
      nodes = getNodes(xpath, expression, inputSource);
      if (nodes.getLength() > 0) SIN =
          Long.parseLong(nodes.item(0).getTextContent());

      expression = "/ClinicalDocument/MedicalRecord/Comments";
      inputAsInputStream.reset();
      nodes = getNodes(xpath, expression, inputSource);
      if (nodes.getLength() > 0) comment = nodes.item(0).getTextContent();
      // end::INTERCEPT1[]

      // tag::INTERCEPT2[]
      byte[] rowKeyBytes =
          Bytes.add(DigestUtils.md5("" + SIN), Bytes.toBytes((int) SIN));
      if (StringUtils.isBlank(firstName) || StringUtils.isBlank(lastName)) {
        LOG.info("Some personal information is missing. Lookup required");
        Table documentsTable = connection.getTable(tableName); // <1>
        Get get = new Get(rowKeyBytes);
        Result result = documentsTable.get(get); // <2>
        if (!result.isEmpty()) {
          result.advance();
          Cell firstCell = result.current();
          Cell currentCell = firstCell;
          while (true) {
            document = cellToAvro(currentCell, document);
            if (document.getFirstName() != null) firstName =
                document.getFirstName().toString();
            if (document.getLastName() != null) lastName =
                document.getLastName().toString();
            if ((!"".equals(firstName) && !"".equals(lastName))
                || (!result.advance())) break; // <3>
            currentCell = result.current();
          }
          if ((firstCell != currentCell) && StringUtils.isNotBlank(lastName)
              && StringUtils.isNotBlank(firstName)) { // <4>
            LOG.info("Need to update first cell. Updating headers.");
            document = cellToAvro(firstCell, document);
            document.setFirstName(firstName);
            document.setLastName(lastName);
            byte[] qualifier =
                Bytes.copy(firstCell.getQualifierArray(),
                  firstCell.getQualifierOffset(), firstCell.getQualifierLength());
            Map<String, String> headers = event.getHeaders();
            headers.put(COLUMN, Base64.encodeBytes(qualifier));
            headers.put(PAYLOAD, Base64.encodeBytes(avroToBytes(document))); // <5>
          }
        }
      }
      // end::INTERCEPT2[]
      // tag::INTERCEPT3[]
      document.setFirstName(firstName);
      document.setLastName(lastName);
      document.setSin(SIN);
      document.setComment(comment);
      // end::INTERCEPT3[]

      byte[] avroAsBytes = avroToBytes(document);
      if (LOG.isInfoEnabled()) {
        inputAsInputStream.reset();
        LOG.info("Changed " + new String(event.getBody()) + " to "
            + new String(avroAsBytes));
      }
      event.setBody(avroAsBytes);
      return event;
    } catch (XPathExpressionException e) {
      LOG.error(e);
    } catch (NumberFormatException e) {
      LOG.error(e);
    } catch (IOException e) {
      LOG.error(e);
    }

    return null;
  }

  @Override
  public List<Event> intercept(final List<Event> events) {
    List<Event> returnedEvents = new ArrayList<Event>(events.size());
    for (Event e : events) {
      Event returned = intercept(e);
      if (returned != null) returnedEvents.add(returned);
    }
    return returnedEvents;
  }

  @Override
  public void close() {
    try {
      connection.close();
    } catch (IOException e) {
      LOG.error(e);
      throw new FlumeException(e);
    }
  }

  public static class Builder implements Interceptor.Builder {
    private Context ctx;

    @Override
    public DocumentInterceptor build() {
      return new DocumentInterceptor(ctx);
    }

    @Override
    public void configure(Context context) {
      this.ctx = context;
    }
  }
}
