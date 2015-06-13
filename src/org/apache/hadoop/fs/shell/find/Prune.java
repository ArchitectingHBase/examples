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
 * Implements -prune expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Prune extends BaseExpression {
  private static final String[] USAGE = {
    "-prune"
  };
  private static final String[] HELP = {
    "Always evaluates to true. Causes the find command to not",
    "descend any further down this directory tree. Does not",
    "have any affect if the -depth expression is specified."
  };

  public Prune() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  @Override
  public Result apply(PathData item) throws IOException {
    if(getOptions().isDepth()) {
      return Result.PASS;
    }
    return Result.STOP;
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Prune.class, "-prune");
  }
}
