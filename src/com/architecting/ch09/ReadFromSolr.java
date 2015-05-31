package com.architecting.ch09;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;


public class ReadFromSolr {
  public static void main(String[] args) throws SolrServerException, IOException {
    CloudSolrServer solr = new CloudSolrServer("localhost:2181/solr");
    solr.setDefaultCollection("Ch09-Collection");
    solr.connect();

    // http://localhost:8983/solr/spellCheckCompRH?q=epod&spellcheck=on&spellcheck.build=true
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/select");
    params.set("q", "docType:ALERT AND partName:NE-555");

    QueryResponse response = solr.query(params);
    SolrDocumentList docs = response.getResults();

    System.out.println("response = " + response);
    System.out.println("response = " + docs.get(0).getFieldValue("eventId"));
  }
}
