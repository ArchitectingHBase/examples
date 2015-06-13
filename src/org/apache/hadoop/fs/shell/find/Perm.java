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

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -perm expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Perm extends BaseExpression {
  private static final String[] USAGE = {
    "-perm [-]mode",
    "-perm [-]onum"
  };
  private static final String[] HELP = {
    "Evaluates as true if the file permissions match that",
    "specified. If the hyphen is specified then the expression",
    "shall evaluate as true if at least the bits specified",
    "match, otherwise an exact match is required.",
    "The mode may be specified using either symbolic notation,",
    "eg 'u=rwx,g+x+w' or as an octal number."
  };

  private int permission;
  private boolean mask = false;
  
  public Perm() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }
  
  /** {@inheritDoc} */
  @Override
  public void initialise(FindOptions options) throws IOException {
    super.initialise(options);
    parseArgument(getArgument(1));
  }
  
  private void parseArgument(String argument) throws IOException {
    String arg = argument;
    if(arg == null) {
      throw new IllegalArgumentException("Null argument");
    }
    if(arg.equals("")) {
      throw new IllegalArgumentException("Empty argument");
    }
    if(arg.startsWith("-")) {
      mask = true;
      arg = arg.substring(1);
    }
    if(Character.isDigit(arg.charAt(0))) {
      permission = new FsPermission(arg).toShort();
    }
    else {
      int shift = 0;
      Operator operator;
      int value = 0;
      for(int i = 0; i < arg.length(); i++) {
        switch(arg.charAt(i++)) {
        case 'u':
          shift = 6;
          break;
        case 'g':
          shift = 3;
          break;
        case 'o':
          shift = 0;
          break;
        default:
          throw new IOException("Invalid mode: " + argument);
        }
        if(i < arg.length()) {
          switch(arg.charAt(i++)) {
          case '=':
            operator = EQUALS;
            break;
          case '+':
            operator = PLUS;
            break;
          case '-':
            operator = MINUS;
            break;
          default:
            throw new IOException("Invalid mode: " + argument);
          }
        }
        else {
          throw new IOException("Invalid mode: " + argument);
        }

        value = 0;
        for(; (i < arg.length()) && (arg.charAt(i) != ','); i++) {
          switch(arg.charAt(i)) {
          case 'r':
            value |= 4;
            break;
          case 'w':
            value |= 2;
            break;
          case 'x':
            value |= 1;
            break;
          default:
            throw new IOException("Invalid mode: " + argument);
          }
        }
        permission = operator.apply(permission, shift, value);
      }
    }
  }
  
  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    int itemPermission = getFileStatus(item).getPermission().toShort();
    if(itemPermission == permission) {
      return Result.PASS;
    }
    else if(mask && ((itemPermission & permission) == permission)) {
      return Result.PASS;
    }
    return Result.FAIL;
  }
  
  /** {@inheritDoc} */
  @Override
  public void addArguments(Deque<String> args) {
    addArguments(args, 1);
  }
  
  private static interface Operator {
    public int apply(int current, int shift, int value);
  }
  private static final Operator EQUALS;
  private static final Operator PLUS;
  private static final Operator MINUS;
  static {
    EQUALS = new Operator() {
      public int apply(int current, int shift, int value) {
        return (current & ~(7 << shift)) | (value << shift);
      };
      public String toString() { return "equals";}
    };
    PLUS = new Operator() {
      public int apply(int current, int shift, int value) {
        return current | (value << shift);
      };
      public String toString() { return "plus";}
    };
    MINUS = new Operator() {
      public int apply(int current, int shift, int value) {
        return current & ~(value << shift);
      };
      public String toString() { return "minus";}
    };
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Perm.class, "-perm");
  }
}
