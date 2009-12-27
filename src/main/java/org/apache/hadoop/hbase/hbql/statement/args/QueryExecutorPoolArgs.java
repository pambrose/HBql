/*
 * Copyright (c) 2009.  The Apache Software Foundation
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

package org.apache.hadoop.hbase.hbql.statement.args;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

public class QueryExecutorPoolArgs extends SelectStatementArgs {


    public QueryExecutorPoolArgs(final GenericValue arg0,
                                 final GenericValue arg1,
                                 final GenericValue arg2,
                                 final GenericValue arg3) {
        super(ArgType.EXECUTORPOOL, arg0, arg1, arg2, arg3);
    }

    public int getMaxPoolSize() throws HBqlException {
        return (Integer)this.evaluateConstant(null, 0, false, null);
    }

    public int getThreadCount() throws HBqlException {
        return (Integer)this.evaluateConstant(null, 1, false, null);
    }

    public boolean getThreadsReadResults() throws HBqlException {
        return (Boolean)this.evaluateConstant(null, 2, false, null);
    }

    public int getQueueSize() throws HBqlException {
        return (Integer)this.evaluateConstant(null, 3, false, null);
    }

    public String asString() {
        try {
            return "MAX_POOL_SIZE : " + this.getMaxPoolSize()
                   + ", THREAD_COUNT : " + this.getThreadCount()
                   + ", THREADS_READ_RESULTS : " + this.getThreadsReadResults()
                   + ", QUEUE_SIZE : " + this.getQueueSize();
        }
        catch (HBqlException e) {
            return "Invalid expression";
        }
    }
}