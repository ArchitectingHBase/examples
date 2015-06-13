/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.dedup.NoChangeUpdateConflictResolver;
import org.apache.solr.hadoop.dedup.RetainMostRecentUpdateConflictResolver;
import org.apache.solr.hadoop.dedup.UpdateConflictResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kitesdk.morphline.api.ExceptionHandler;
import org.kitesdk.morphline.base.FaultTolerance;
import com.google.common.base.Preconditions;

/**
 * This class loads the mapper's SolrInputDocuments into one EmbeddedSolrServer
 * per reducer. Each such reducer and Solr server can be seen as a (micro)
 * shard. The Solr servers store their data in HDFS.
 * 
 * More specifically, this class consumes a list of &lt;docId, SolrInputDocument&gt;
 * pairs, sorted by docId, and sends them to an embedded Solr server to generate
 * a Solr index shard from the documents.
 */
public class SolrReducer extends Reducer<Text, SolrInputDocumentWritable, Text, SolrInputDocumentWritable> {

  private HeartBeater heartBeater;
  private ExceptionHandler exceptionHandler;
  
  private static final Logger LOG = LoggerFactory.getLogger(SolrReducer.class);
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    verifyPartitionAssignment(context);    
    SolrRecordWriter.addReducerContext(context);

    /*
     * Note that ReflectionUtils.newInstance() above also implicitly calls
     * resolver.configure(context.getConfiguration()) if the resolver
     * implements org.apache.hadoop.conf.Configurable
     */

    this.exceptionHandler = new FaultTolerance(
        context.getConfiguration().getBoolean(FaultTolerance.IS_PRODUCTION_MODE, false), 
        context.getConfiguration().getBoolean(FaultTolerance.IS_IGNORING_RECOVERABLE_EXCEPTIONS, false),
        context.getConfiguration().get(FaultTolerance.RECOVERABLE_EXCEPTION_CLASSES, SolrServerException.class.getName()));
    
    this.heartBeater = new HeartBeater(context);
  }
  
  protected void reduce(Text key, Iterable<SolrInputDocumentWritable> values, Context context) throws IOException, InterruptedException {
    heartBeater.needHeartBeat();
    try {
      super.reduce(key, values, context);
    } catch (Exception e) {
      LOG.error("Unable to process key " + key, e);
      context.getCounter(getClass().getName() + ".errors", e.getClass().getName()).increment(1);
      exceptionHandler.handleException(e, null);
    } finally {
      heartBeater.cancelHeartBeat();
    }
  }


  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    heartBeater.close();
    super.cleanup(context);
  }

  /*
   * Verify that if a mappers's partitioner sends an item to partition X it implies that said item
   * is sent to the reducer with taskID == X. This invariant is currently required for Solr
   * documents to end up in the right Solr shard.
   */
  private void verifyPartitionAssignment(Context context) {
    if ("true".equals(System.getProperty("verifyPartitionAssignment", "true"))) {
      String partitionStr = context.getConfiguration().get("mapred.task.partition");
      if (partitionStr == null) {
        partitionStr = context.getConfiguration().get("mapreduce.task.partition");
      }
      int partition = Integer.parseInt(partitionStr);
      int taskId = context.getTaskAttemptID().getTaskID().getId();
      Preconditions.checkArgument(partition == taskId, 
          "mapred.task.partition: " + partition + " not equal to reducer taskId: " + taskId);      
    }
  }
}
