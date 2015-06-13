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

import java.io.IOException;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -print expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Print extends BaseExpression {
  private static final String[] USAGE = {
    "-print",
    "-print0"
  };
  private static final String[] HELP = {
    "Always evaluates to true. Causes the current pathname to be",
    "written to standard output. If the -print0 expression is",
    "used then an ASCII NULL character is appended.",
  };
  
  private boolean appendNull;

  private void setAppendNull(boolean appendNull) {
    this.appendNull = appendNull;
  }
  
  public Print(boolean appendNull) {
    super();
    setUsage(USAGE);
    setHelp(HELP);
    setAppendNull(appendNull);
  }
  
  public Print() {
    this(false);
  }

  @Override
  public Result apply(PathData item) throws IOException {
    getOptions().getOut().println(getPath(item).toString() + appendNull());
    return Result.PASS;
  }
  
  private String appendNull() {
    return (appendNull ? "\0" : "");
  }
  @Override
  public boolean isAction() {
    return true;
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Print.class, "-print");
    factory.addClass(Print0.class, "-print0");
  }
  
  /** Implements the -print0 expression. */
  public final static class Print0 extends FilterExpression {
    public Print0() {
      super(new Print(true));
    }
  }
}
