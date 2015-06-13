/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.shell.find;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.Date;

import org.apache.hadoop.fs.shell.CommandFactory;

/**
 * Options to be used by the {@link Find} command and its {@link Expression}s.
 */
public class FindOptions {
  /** Output stream to be used. */
  private PrintStream out;
  
  /** Error stream to be used. */
  private PrintStream err;
  
  /** Input stream to be used. */
  private InputStream in;
  
  /** Indicates whether the expression should be applied to the directory tree depth first. */ 
  private boolean depth = false;
  
  /** Indicates whether symbolic links should be followed. */
  private boolean followLink = false;
  
  /** Indicates whether symbolic links specified as command arguments should be followed. */
  private boolean followArgLink = false;
  
  /** Start time of the find process. */
  private long startTime = new Date().getTime();

  /** Factory for retrieving command classes. */
  private CommandFactory commandFactory;
  /** 
   * Sets the output stream to be used.
   * @param out output stream to be used
   */
  public void setOut(PrintStream out) {
    this.out = out;
  }
  
  /**
   * Returns the output stream to be used.
   * @return output stream to be used
   */
  public PrintStream getOut() {
    return this.out;
  }
  
  /**
   * Sets the error stream to be used.
   * @param err error stream to be used
   */
  public void setErr(PrintStream err) {
    this.err = err;
  }
  
  /**
   * Returns the error stream to be used.
   * @return error stream to be used
   */
  public PrintStream getErr() {
    return this.err;
  }
  
  /**
   * Sets the input stream to be used.
   * @param in input stream to be used
   */
  public void setIn(InputStream in) {
    this.in = in;
  }
  
  /**
   * Returns the input stream to be used.
   * @return input stream to be used
   */
  public InputStream getIn() {
    return this.in;
  }
  
  /**
   * Sets flag indicating whether the expression should be applied to the directory tree depth first.
   * @param depth true indicates depth first traversal
   */
  public void setDepth(boolean depth) {
    this.depth = depth;
  }
  
  /**
   * Should directory tree be traversed depth first?
   * @return true indicate depth first traversal
   */
  public boolean isDepth() {
    return this.depth;
  }
  
  /**
   * Sets flag indicating whether symbolic links should be followed.
   * @param followLink true indicates follow links
   */
  public void setFollowLink(boolean followLink) {
    this.followLink = followLink;
  }
  
  /**
   * Should symbolic links be follows?
   * @return true indicates links should be followed
   */
  public boolean isFollowLink() {
    return this.followLink;
  }
  
  /**
   * Sets flag indicating whether command line symbolic links should be followed.
   * @param followArgLink true indicates follow links
   */
  public void setFollowArgLink(boolean followArgLink) {
    this.followArgLink = followArgLink;
  }
  
  /**
   * Should command line symbolic links be follows?
   * @return true indicates links should be followed
   */
  public boolean isFollowArgLink() {
    return this.followArgLink;
  }
  
  /**
   * Returns the start time of this {@link Find} command.
   * @return start time (in milliseconds since epoch)
   */
  public long getStartTime() {
    return this.startTime;
  }
  
  /**
   * Set the start time of this {@link Find} command.
   * @param time start time (in milliseconds since epoch)
   */
  public void setStartTime(long time) {
    this.startTime = time;
  }
  
  /**
   * Set the command factory.
   * @param factory {@link CommandFactory}
   */
  public void setCommandFactory(CommandFactory factory) {
    this.commandFactory = factory;
  }
  
  /**
   * Return the command factory.
   * @return {@link CommandFactory}
   */
  public CommandFactory getCommandFactory() {
    return this.commandFactory;
  }
}
