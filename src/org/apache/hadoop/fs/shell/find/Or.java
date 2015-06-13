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

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -o (or) operator for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Or extends BaseExpression {
  private static final String[] USAGE = {
    "expression -o expression",
    "expression -or expression"
  };
  private static final String[] HELP = {
    "Logical OR operator for joining two expressions. Returns",
    "true if one of the child expressions returns true. The",
    "second expression will not be applied if the first returns",
    "true."
  };

  public Or() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    for(Expression child : getChildren()) {
      Result result = child.apply(item);
      if(result.isPass()) {
        return result;
      }
    }
    return Result.FAIL;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isOperator() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public int getPrecedence() {
    return 100;
  }
  
  /** {@inheritDoc} */
  @Override
  public void addChildren(Deque<Expression> expressions) {
    addChildren(expressions, 2);
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Or.class, "-o");
    factory.addClass(Or.class, "-or");
  }
}
