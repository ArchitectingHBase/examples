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

public final class Result {
  public static final Result PASS = new Result(true, true);
  public static final Result FAIL = new Result(false, true);
  public static final Result STOP = new Result(true, false);
  private boolean descend;
  private boolean success;
  private Result(boolean success, boolean recurse) {
    this.success = success;
    this.descend = recurse;
  }
  public boolean isDescend() {
    return this.descend;
  }
  public boolean isPass() {
    return this.success;
  }
  public Result combine(Result other) {
    return new Result(this.isPass() && other.isPass(), this.isDescend() && other.isDescend());
  }
  public Result negate() {
    return new Result(!this.isPass(), this.isDescend());
  }
  public String toString() {
    return "success=" + isPass() + "; recurse=" + isDescend();
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (descend ? 1231 : 1237);
    result = prime * result + (success ? 1231 : 1237);
    return result;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Result other = (Result) obj;
    if (descend != other.descend)
      return false;
    if (success != other.success)
      return false;
    return true;
  }
}
