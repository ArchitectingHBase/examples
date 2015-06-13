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
 * Implements the -atime expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Atime extends NumberExpression {
  private static final String[] USAGE = {
    "-atime n",
    "-amin n"
  };
  private static final String[] HELP = {
    "Evaluates as true if the file access time subtracted from",
    "the start time is n days (or minutes if -amin is used)."
  };
  
  public Atime() {
    this(DAY_IN_MILLISECONDS);
  }
  public Atime(long units) {
    super(units);
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    return applyNumber(getOptions().getStartTime() - getFileStatus(item).getAccessTime());
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Atime.class, "-atime");
    factory.addClass(Amin.class, "-amin");
  }
  
  /** Implement -amin expression (similar to -atime but in minutes). */
  static class Amin extends FilterExpression {
    public Amin() {
      super(new Atime(MINUTE_IN_MILLISECONDS));
    }
  }
}
