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
 * Implements the ! (not) operator for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Not extends BaseExpression {
  private static final String[] USAGE = {
    "! expression",
    "-not expression"
  };
  private static final String[] HELP = {
    "Evaluates as true if the expression evaluates as false and",
    "vice-versa."
  };

  public Not() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    Expression child = getChildren().get(0);
    Result result = child.apply(item);
    return result.negate();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isOperator() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public int getPrecedence() {
    return 300;
  }

  /** {@inheritDoc} */
  @Override
  public void addChildren(Deque<Expression> expressions) {
    addChildren(expressions, 1);
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Not.class, "!");
    factory.addClass(Not.class, "-not");
  }
}
