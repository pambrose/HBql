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

package org.apache.hadoop.hbase.hbql.impl;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.QueryExecutorPool;
import org.apache.hadoop.hbase.hbql.util.ExecutorWithQueue;

public class ResultExecutor extends ExecutorWithQueue<Result> {

    private ResultExecutor(final QueryExecutorPool executorPool, final int threadCount, final int queueSize) {
        super(executorPool, threadCount, queueSize);
    }

    public static ResultExecutor newPooledResultExecutor(final QueryExecutorPool executorPool,
                                                         final int threadCount,
                                                         final int queueSize) {
        return new ResultExecutor(executorPool, threadCount, queueSize);
    }

    public static ResultExecutor newResultExecutor(final int threadCount, final int queueSize) {
        return new ResultExecutor(null, threadCount, queueSize);
    }

    public boolean threadsReadResults() {
        return true;
    }
}