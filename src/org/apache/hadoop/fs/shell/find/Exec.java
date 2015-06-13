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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -exec and -ok expressions for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Exec extends BaseExpression {
  private static final int MAX_ARGS = 500;
  private static final String[] USAGE = {
    "-exec command [argument ...]",
    "-ok command [argument ...]"
  };
  private static final String[] HELP = {
    "Executes the specified Hadoop shell command with the given",
    "arguments. If the string {} is given as an argument then",
    "is replaced by the current path name.  If a {} argument is",
    "followed by a + character then multiple paths will be",
    "batched up and passed to a single execution of the command.",
    "A maximum of " + MAX_ARGS + " paths will be passed to a single",
    "command. The expression evaluates to true if the command",
    "returns success and false if it fails.",
    "If -ok is specified then confirmation of each command shall be",
    "prompted for on STDERR prior to execution.  If the response is",
    "'y' or 'yes' then the command shall be executed else the command",
    "shall not be invoked and the expression shall return false."
  };
  private Command command;
  private ArrayList<String> pathItems;
  private boolean batch = false;
  private int maxArgs = MAX_ARGS;
  private boolean prompt;
  private BufferedReader reader = null;

  public Exec() {
    this(false);
  }
  public Exec(boolean prompt) {
    super();
    setUsage(USAGE);
    setHelp(HELP);
    setPrompt(prompt);
  }
  
  private void setPrompt(boolean prompt) {
    this.prompt = prompt;
  }
  private boolean getPrompt() {
    return this.prompt;
  }

  private BufferedReader getReader() throws IOException {
    if(reader == null) {
      reader = new BufferedReader(new InputStreamReader(getOptions().getIn()));
    }
    return reader;
  }
  @Override
  public void addArguments(Deque<String> args) {
    while(!args.isEmpty()) {
      String arg = args.pop();
      if("+".equals(arg) &&
          !getArguments().isEmpty() &&
          "{}".equals(getArguments().get(getArguments().size() - 1))) {
        // + terminates the arguments if the previous argument was {}
        setBatch(true);
        break;
      }
      else if(";".equals(arg)) {
        // ; terminates the arguments
        break;
      }
      else {
        addArgument(arg);
      }
    }
  }
  
  @Override
  public void initialise(FindOptions options) throws IOException {
    super.initialise(options);
    String commandName = getArgument(1);
    this.command = getOptions().getCommandFactory().getInstance(commandName);
    if(this.command == null) {
      throw new IOException("Unknown command: " + commandName);
    }
    pathItems = new ArrayList<String>();
  }
  
  /** Build and run the command. */
  private Result runCommand(List<String> items) throws IOException {
    ArrayList<String> commandArgs = new ArrayList<String>();
    boolean gotCommand = false;
    for(String arg : getArguments()) {
      if(gotCommand) {
        if("{}".equals(arg)) {
          commandArgs.addAll(items);
        }
        else {
          commandArgs.add(arg);
        }
      }
      else {
        gotCommand = true;
      }
    }
    pathItems.clear();
    if(getPrompt()) {
      StringBuilder commandString = new StringBuilder();
      commandString.append(command.getName());
      for(String arg : commandArgs) {
        commandString.append(" ").append(arg);
      }
      getOptions().getErr().print("\"" + commandString.toString() + "\"?");
      String response = getReader().readLine();
      if(!("y".equalsIgnoreCase(response) || "yes".equalsIgnoreCase(response))) {
        return Result.FAIL;
      }
    }
    return command.run(commandArgs.toArray(new String[0])) == 0 ? Result.PASS : Result.FAIL;
  }
  @Override
  public Result apply(PathData item) throws IOException {
    if(isBatch()) {
      pathItems.add(getPath(item).toString());
      if(pathItems.size() >= getMaxArgs()) {
        runCommand(pathItems);
      }
      return Result.PASS;
    }
    return runCommand(Arrays.asList(getPath(item).toString()));
  }
  @Override
  public void finish() throws IOException {
    if(pathItems.size() > 0) {
      runCommand(pathItems);
    }
  }

  @Override
  public boolean isAction() {
    return true;
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Exec.class, "-exec");
    factory.addClass(Ok.class, "-ok");
  }
  
  Command getCommand() {
    return this.command;
  }
  void setMaxArgs(int maxArgs) {
    this.maxArgs = maxArgs;
  }
  int getMaxArgs() {
    return this.maxArgs;
  }
  void setBatch(boolean batch) {
    this.batch = batch;
  }
  boolean isBatch() {
    return this.batch;
  }
  
  public static final class Ok extends FilterExpression {
    public Ok() {
      super(new Exec(true));
    }
  }
}