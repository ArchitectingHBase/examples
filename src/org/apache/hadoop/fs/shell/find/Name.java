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
import java.util.Deque;

import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -name expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Name extends BaseExpression {
  private static final String[] USAGE = {
    "-name pattern",
    "-iname pattern"
  };
  private static final String[] HELP = {
    "Evaluates as true if the basename of the file matches the",
    "pattern using standard file system globbing.",
    "If -iname is used then the match is case insensitive."
  };
  private GlobPattern globPattern;
  private boolean caseSensitive = true;

  public Name() {
    this(true);
  }
  public Name(boolean caseSensitive) {
    super();
    setUsage(USAGE);
    setHelp(HELP);
    setCaseSensitive(caseSensitive);
  }
  
  private void setCaseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }
  
  /** {@inheritDoc} */
  @Override
  public void addArguments(Deque<String> args) {
    addArguments(args, 1);
  }
  
  /** {@inheritDoc} */
  @Override
  public void initialise(FindOptions options) {
    String argPattern = getArguments().get(0);
    if(!caseSensitive) {
      argPattern = argPattern.toLowerCase();
    }
    globPattern = new GlobPattern(argPattern);
  }

  @Override
  public Result apply(PathData item) {
    String name = getPath(item).getName();
    if(!caseSensitive) {
      name = name.toLowerCase();
    }
    if(globPattern.matches(name)) {
      return Result.PASS;
    }
    else {
      return Result.FAIL;
    }
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Name.class, "-name");
    factory.addClass(Iname.class, "-iname");
  }
  
  /** Case insensitive version of the -name expression. */
  public static class Iname extends FilterExpression {
    public Iname() {
      super(new Name(false));
    }
  }
}
