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
import org.apache.hadoop.hbase.hbql.util.CompletionQueueExecutor;

public class ResultExecutor extends CompletionQueueExecutor<Result> {

    private ResultExecutor(final QueryExecutorPool executorPool,
                           final int minThreadCount,
                           final int maxThreadCount,
                           final long keepAliveSecs,
                           final int completionQueueSize) {
        super(executorPool, minThreadCount, maxThreadCount, keepAliveSecs, completionQueueSize);
    }

    public static ResultExecutor newPooledResultExecutor(final QueryExecutorPool executorPool,
                                                         final int minThreadCount,
                                                         final int maxThreadCount,
                                                         final long keepAliveSecs,
                                                         final int completionQueueSize) {
        return new ResultExecutor(executorPool, minThreadCount, maxThreadCount, keepAliveSecs, completionQueueSize);
    }

    public static ResultExecutor newResultExecutor(final int minThreadCount,
                                                   final int maxThreadCount,
                                                   final long keepAliveSecs,
                                                   final int completionQueueSize) {
        return new ResultExecutor(null, minThreadCount, maxThreadCount, keepAliveSecs, completionQueueSize);
    }

    public boolean threadsReadResults() {
        return true;
    }
}