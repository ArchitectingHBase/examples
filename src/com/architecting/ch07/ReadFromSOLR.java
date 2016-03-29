package com.architecting.ch07;

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
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.io.IOException;

public class ReadFromSOLR {

  public static final TableName sensorsTableName = TableName.valueOf("sensors");
  public static final Log LOG = LogFactory.getLog(ReadFromSOLR.class);

  public static void main(String[] args) throws SolrServerException, IOException {
    // tag::SETUP[]
    CloudSolrServer solr = new CloudSolrServer("localhost:2181/solr"); // <1>
    solr.setDefaultCollection("Ch09-Collection"); // <2>
    solr.connect();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/select");
    params.set("q", "docType:ALERT AND partName:NE-555"); // <3>

    QueryResponse response = solr.query(params); // <4>
    SolrDocumentList docs = response.getResults();

    LOG.info("Found " + docs.getNumFound() + " matching documents.");
    if (docs.getNumFound() == 0) return;
    byte[] firstRowKey = (byte[]) docs.get(0).getFieldValue("rowkey");
    LOG.info("First document rowkey is " + Bytes.toStringBinary(firstRowKey));

    // Retrieve and print the first 10 columns of the first returned document
    Configuration config = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();
        Table sensorsTable = connection.getTable(sensorsTableName)) {
      Get get = new Get(firstRowKey); // <5>

      Result result = sensorsTable.get(get);
      Event event = null;
      if (result != null && !result.isEmpty()) { // <6>
        for (int index = 0; index < 10; index++) { // Print first 10 columns
          if (!result.advance())
            break; // There are no more columns and we have not reached 10.
          event = new Util().cellToEvent(result.current(), event);
          LOG.info("Retrieved AVRO content: " + event.toString());
        }
      } else {
        LOG.error("Impossible to find requested cell");
      }
    }
    // end::SETUP[]
  }
}
