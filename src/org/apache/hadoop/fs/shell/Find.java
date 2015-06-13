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
package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.*;

/**
 * Count the number of directories, files, bytes, quota, and remaining quota.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

/**
 * Implements a Hadoop find command.
 */
public class Find extends FsCommand {
  public static final String NAME = "find";
  public static final String USAGE = "<path> ... <expression> ...";
  public static final String DESCRIPTION;
  private static String[] HELP = {
    "Finds all files that match the specified expression and applies selected actions to them."
  };
  
  private static final String OPTION_FOLLOW_LINK = "L";
  private static final String OPTION_FOLLOW_ARG_LINK = "H";
  
  /** List of expressions recognised by this command. */
  @SuppressWarnings("rawtypes")
  private static final Class[] EXPRESSIONS = {
    Atime.class,
    Blocksize.class,
    ClassExpression.class,
    Depth.class,
    Empty.class,
    Exec.class,
    Group.class,
    Mtime.class,
    Name.class,
    Newer.class,
    Nogroup.class,
    Nouser.class,
    Perm.class,
    Print.class,
    Prune.class,
    Replicas.class,
    Size.class,
    Type.class,
    User.class,
    And.class,
    Or.class,
    Not.class,
  };
  
  /** Options for use in this command */
  private FindOptions options;
  
  /** Root expression for this instance of the command. */
  private Expression rootExpression;
  
  /** Set of links followed to guard against infinite loops. */
  private HashSet<PathData> linksFollowed = new HashSet<PathData>();
  
  /** allows the command factory to be used if necessary */
  private CommandFactory commandFactory = null;
  
  /** sets the command factory for later use */
  public void setCommandFactory(CommandFactory factory) { // FIXME FsShell should call this
    this.commandFactory = factory;
  }
  
  /** retrieves the command factory */
  protected CommandFactory getCommandFactory() {
    return this.commandFactory;
  }
  
  /**
   * Register the names for the count command
   * @param factory the command factory that will instantiate this class
   */
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Find.class, "-find");
  }

/** Register the expressions with the expression factory. */
  static {
    registerExpressions(ExpressionFactory.getExpressionFactory());
  }
  
  /** Build the description used by the help command. */
  static {
    DESCRIPTION = buildDescription(ExpressionFactory.getExpressionFactory());
  }
  
  /** Register the expressions with the expression factory. */
  @SuppressWarnings("unchecked")
  private static void registerExpressions(ExpressionFactory factory) {
    for(Class<? extends Expression> exprClass : EXPRESSIONS) {
      factory.registerExpression(exprClass);
    }
  }
  
  /** Build the description used by the help command. */
  @SuppressWarnings("unchecked")
  private static String buildDescription(ExpressionFactory factory) {
    ArrayList<Expression> operators = new ArrayList<Expression>();
    ArrayList<Expression> primaries = new ArrayList<Expression>();
    for(Class<? extends Expression> exprClass : EXPRESSIONS) {
      Expression expr = factory.createExpression(exprClass, null);
      if(expr.isOperator()) {
        operators.add(expr);
      }
      else {
        primaries.add(expr);
      }
    }
    Collections.sort(operators, new Comparator<Expression>() {
      @Override
      public int compare(Expression arg0, Expression arg1) {
        return arg0.getClass().getName().compareTo(arg1.getClass().getName());
      }});
    Collections.sort(primaries, new Comparator<Expression>() {
      @Override
      public int compare(Expression arg0, Expression arg1) {
        return arg0.getClass().getName().compareTo(arg1.getClass().getName());
      }});

    StringBuilder sb = new StringBuilder();
    for(String line : HELP) {
      sb.append(line).append("\n");
    }
    sb.append("\n");
    sb.append("The following primary expressions are recognised:\n");
    for(Expression expr : primaries) {
      for(String line : expr.getUsage()) {
        sb.append("  ").append(line).append("\n");
      }
      for(String line : expr.getHelp()) {
        sb.append("    ").append(line).append("\n");
      }
      sb.append("\n");
    }
    sb.append("The following operators are recognised:\n");
    for(Expression expr : operators) {
      for(String line : expr.getUsage()) {
        sb.append("  ").append(line).append("\n");
      }
      for(String line : expr.getHelp()) {
        sb.append("    ").append(line).append("\n");
      }
      sb.append("\n");
    }
    return sb.toString();
  }

  protected void processOptions(LinkedList<String> args) throws IOException {
    CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE, OPTION_FOLLOW_LINK, OPTION_FOLLOW_ARG_LINK);
    cf.parse(args);
    
    if(cf.getOpt(OPTION_FOLLOW_LINK)) {
      options.setFollowLink(true);
    }
    else if(cf.getOpt(OPTION_FOLLOW_ARG_LINK)) {
      options.setFollowArgLink(true);
    }
  }
  
  /**
   * Set the root expression.
   * @param expression
   */
  @InterfaceAudience.Private
  void setRootExpression(Expression expression) {
    this.rootExpression = expression;
  }
  
  /**
   * Return the root expression.
   * @return the root expression
   */
  @InterfaceAudience.Private
  Expression getRootExpression() {
    return this.rootExpression;
  }

  /** Returns the current find options, creating them if necessary. */
  @InterfaceAudience.Private
  FindOptions getOptions() {
    if(options == null) {
      options = new FindOptions();
      options.setOut(out);
      options.setErr(err);
      options.setIn(System.in);
      options.setCommandFactory(getCommandFactory());
    }
    return options;
  }

  /**
   * Parse a list of arguments to extract the {@link PathData} elements.
   * The input deque will be modified to remove the used elements.
   * @param args arguments to be parsed
   * @return list of {@link PathData} elements applicable to this command
   * @throws IOException if list can not be parsed
   */
  private LinkedList<PathData> parsePathData(Deque<String> args) throws IOException {
    LinkedList<PathData> pathArgs = new LinkedList<PathData>();
    while(!args.isEmpty()) {
      String arg = args.pop();
      if(isExpression(arg) || "(".equals(arg) || (arg == null) || arg.startsWith("-")) {
        args.push(arg);
        return pathArgs;
      }
      pathArgs.addAll(expandArgument(arg));
    }
    return pathArgs;
  }
  
  /**
   * Parse a list of arguments to to extract the {@link Expression} elements.
   * The input list will be modified to remove the used elements.
   * @param args arguments to be parsed
   * @return list of {@link Expression} elements applicable to this command
   * @throws IOException if list can not be parsed
   */
  private Expression parseExpression(Deque<String> args) throws IOException {
    Deque<Expression> primaries = new LinkedList<Expression>();
    Deque<Expression> operators = new LinkedList<Expression>();
    Expression prevExpr = getExpression(And.class);
    while(!args.isEmpty()) {
      String arg = args.pop();
      if("(".equals(arg)) {
        Expression expr = parseExpression(args);
        primaries.add(expr);
        prevExpr = new BaseExpression(){}; // stub the previous expression to be a non-op
      }
      else if(")".equals(arg)) {
        break;
      }
      else if(isExpression(arg)) {
        Expression expr = getExpression(arg);
        expr.addArguments(args);
        if(expr.isOperator()) {
          while(!operators.isEmpty()) {
            if(operators.peek().getPrecedence() >= expr.getPrecedence()) {
              Expression op = operators.pop();
              op.addChildren(primaries);
              primaries.push(op);
            }
            else {
              break;
            }
          }
          operators.push(expr);
        }
        else {
          if(!prevExpr.isOperator()) {
            Expression and = getExpression(And.class);
            while(!operators.isEmpty()) {
              if(operators.peek().getPrecedence() >= and.getPrecedence()) {
                Expression op = operators.pop();
                op.addChildren(primaries);
                primaries.push(op);
              }
              else {
                break;
              }
            }
            operators.push(and);
          }
          primaries.push(expr);
        }
        prevExpr = expr;
      }
      else {
        throw new IOException("Unexpected argument: " + arg);
      }
    }
    
    while(!operators.isEmpty()) {
      Expression operator = operators.pop();
      operator.addChildren(primaries);
      primaries.push(operator);
    }

    return primaries.isEmpty() ? getExpression(Print.class) : primaries.pop();
  }
  
  /** {@inheritDoc} */
  @Override
  protected LinkedList<PathData> expandArguments(LinkedList<String> argList) throws IOException {
    Deque<String> args = new LinkedList<String>(argList);
    LinkedList<PathData> pathArgs = parsePathData(args);
    if(pathArgs.size() == 0) {
      throw new IOException("No path specified");
    }
    Expression expression = parseExpression(args);
    if(!expression.isAction()) {
      Expression and = getExpression(And.class);
      Deque<Expression> children = new LinkedList<Expression>();
      children.add(getExpression(Print.class));
      children.add(expression);
      and.addChildren(children);
      expression = and;
    }
    
    setRootExpression(expression);
    
    return pathArgs;
  }
  
  @Override
  protected void recursePath(PathData item) throws IOException {
    if(item.stat.isSymlink() && getOptions().isFollowLink()) {
      PathData linkedItem = new PathData(item.stat.getSymlink().toString(), getConf());
      if(linksFollowed.contains(item)) {
        getOptions().getErr().println("Infinite loop ignored: " + item.toString() + " -> " + linkedItem.toString());
        return;
      }
      linksFollowed.add(item);
      item = linkedItem;
    }
    if(item.stat.isDirectory()) {
      super.recursePath(item);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void processPaths(PathData parent, PathData ... items) throws IOException {
    if(parent == null) {
      // processing a command line argument so clear the links followed
      linksFollowed.clear();
    }
    Expression expr = getRootExpression();
    for (PathData item : items) {
      try {
        if(getOptions().isDepth()) {
          recursePath(item);
          expr.apply(item);
        }
        else if (expr.apply(item).isDescend()) {
            recursePath(item);
        }
      } catch (IOException e) {
        displayError(e);
      }
    }
  }
  
  /** {@inheritDoc} */
  @Override
  protected void processArguments(LinkedList<PathData> args) throws IOException {
    Expression expr = getRootExpression();
    expr.initialise(getOptions());
    super.processArguments(args);
    expr.finish();
  }
  
  /** {@inheritDoc} */
  @Override
  protected void processPathArgument(PathData item) throws IOException {
    if(item.stat.isSymlink() && (getOptions().isFollowArgLink() || getOptions().isFollowLink())) {
      item = new PathData(item.stat.getSymlink().toString(), getConf());
    }
    super.processPathArgument(item);
  }
  /** Gets a named expression from the factory. */
  private Expression getExpression(String expressionName) {
    return ExpressionFactory.getExpressionFactory().getExpression(expressionName, getConf());
  }
  
  /** Gets an instance of an expression from the factory. */
  private Expression getExpression(Class<? extends Expression> expressionClass) {
    return ExpressionFactory.getExpressionFactory().createExpression(expressionClass, getConf());
  }
  
  /** Asks the factory whether an expression is recognised. */
  private boolean isExpression(String expressionName) {
    return ExpressionFactory.getExpressionFactory().isExpression(expressionName);
  }
}