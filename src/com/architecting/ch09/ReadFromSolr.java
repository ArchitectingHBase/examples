package com.architecting.ch09;

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


public class ReadFromSolr {

  public static final TableName sensorsTableName = TableName.valueOf("sensors");
  public static final Log LOG = LogFactory.getLog(ReadFromSolr.class);

  public static void main(String[] args) throws SolrServerException, IOException {
    CloudSolrServer solr = new CloudSolrServer("localhost:2181/solr");
    solr.setDefaultCollection("Ch09-Collection");
    solr.connect();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/select");
    params.set("q", "docType:ALERT AND partName:NE-555");

    QueryResponse response = solr.query(params);
    SolrDocumentList docs = response.getResults();

    LOG.info("Found " + docs.getNumFound() + " matching documents.");
    if (docs.getNumFound() == 0)
      return;
    LOG.info("First document rowkey is " +
            Bytes.toStringBinary((byte[])docs.get(0).getFieldValue("rowkey")));

    // Retrieve and print the first 10 columns of the first returned document
    Configuration config = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();
        Table sensorsTable = connection.getTable(sensorsTableName)) {  // <1>
     byte[] rowKey = (byte[])(docs.get(0).getFieldValue("rowkey"));
     Get get = new Get (rowKey);

     Result result = sensorsTable.get(get);
     Event event = null;
     if (result != null && !result.isEmpty()) { // <4>
       for (int index = 0; index < 10; index++) { // Print first 10 columns
         if (!result.advance())
           break; // The is no more column.
         event = new Util().cellToEvent(result.current(),event); // <5>
         LOG.info("Retrieved AVRO content: " + event.toString());
       }
     } else {
       LOG.error("Impossible to find requested cell");
     }
   }
  
  }
}
