/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.solr.hadoop;

import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.Find;
import org.apache.hadoop.util.ToolRunner;

/**
 * HDFS version of Linux 'find' command line tool. Borrowed from
 * https://issues.apache.org/jira/browse/HADOOP-8989
 *
 * Example usage:
 * <pre>
 * sudo -u hdfs hadoop --config /etc/hadoop/conf.cloudera.mapreduce1 jar search-mr-*-job.jar org.apache.solr.hadoop.HdfsFindTool -help
 * 
 * sudo -u hdfs hadoop --config /etc/hadoop/conf.cloudera.mapreduce1 jar search-mr-*-job.jar org.apache.solr.hadoop.HdfsFindTool -find hdfs:///user/$USER/solrloadtest/iarchive/1percent/WIDE-20110309005125-crawl338 hdfs:///user/$USER/solrloadtest/iarchive/1percent/WIDE-20110309002853-crawl337 -name '*.gz' -type f
 * </pre>
 */
public class HdfsFindTool extends FsShell {
  
  public static void main(String argv[]) throws Exception {
    FsShell shell = new HdfsFindTool();
    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }

  @Override
  protected void registerCommands(CommandFactory factory) {
    super.registerCommands(factory);
    factory.registerCommands(Find.class);
  }
}

