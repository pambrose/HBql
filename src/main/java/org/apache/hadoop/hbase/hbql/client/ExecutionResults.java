/*
 * Copyright (c) 2010.  The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.hbql.client;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class ExecutionResults {

    private final ByteArrayOutputStream baos;
    public final PrintStream out;

    private boolean success = true;
    private int count = -1;
    private boolean predicate = true;

    public ExecutionResults() {
        baos = new ByteArrayOutputStream();
        out = new PrintStream(baos);
    }

    public ExecutionResults(final String str) {
        this();
        this.out.println(str);
    }

    public boolean hadSuccess() {
        return this.success;
    }

    public void setSuccess(final boolean success) {
        this.success = success;
    }

    public String toString() {
        this.out.flush();
        return baos.toString();
    }

    public int getCount() {
        return this.count;
    }

    public void setCount(final int count) {
        this.count = count;
    }

    public boolean getPredicate() {
        return this.predicate;
    }

    public void setPredicate(final boolean predicate) {
        this.predicate = predicate;
    }
}
