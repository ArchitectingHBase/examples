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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.PathData;

/**
 * Provides an abstract composition filter for the {@link Expression} interface.
 * Allows other {@link Expression} implementations to be reused without inheritance.
 */
public abstract class FilterExpression implements Expression, Configurable {
  protected Expression expression;
  protected FilterExpression(Expression expression) {
    this.expression = expression;
  }

  /** {@inheritDoc} */
  @Override
  public void initialise(FindOptions options) throws IOException {
    expression.initialise(options);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    return expression.apply(item);
  }

  /** {@inheritDoc} */
  @Override
  public void finish() throws IOException {
    expression.finish();
  }

  /** {@inheritDoc} */
  @Override
  public String[] getUsage() {
    return expression.getUsage();
  }

  /** {@inheritDoc} */
  @Override
  public String[] getHelp() {
    return expression.getHelp();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isAction() {
    return expression.isAction();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isOperator() {
    return expression.isOperator();
  }

  /** {@inheritDoc} */
  @Override
  public int getPrecedence() {
    return expression.getPrecedence();
  }

  /** {@inheritDoc} */
  @Override
  public void addChildren(Deque<Expression> expressions) {
    expression.addChildren(expressions);
  }

  /** {@inheritDoc} */
  @Override
  public void addArguments(Deque<String> args) {
    expression.addArguments(args);
  }

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    if(expression instanceof Configurable) {
      ((Configurable)expression).setConf(conf);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    if(expression instanceof Configurable) {
      return ((Configurable)expression).getConf();
    }
    return null;
  }
  
  /** {@inheritDoc} */
  @Override
  public String toString() {
    return getClass().getSimpleName() + "-" + expression.toString();
  }
}
