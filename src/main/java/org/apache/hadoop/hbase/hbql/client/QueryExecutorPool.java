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

package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.impl.ResultExecutor;
import org.apache.hadoop.hbase.hbql.impl.ResultScannerExecutor;
import org.apache.hadoop.hbase.hbql.util.CompletionQueueExecutor;
import org.apache.hadoop.hbase.hbql.util.ElementPool;

public class QueryExecutorPool extends ElementPool<CompletionQueueExecutor> {

    private final int coreThreadCount;
    private final int maxThreadCount;
    private final boolean threadsReadResults;
    private final int completionQueueSize;

    public QueryExecutorPool(final String name,
                             final int maxExecutorPoolSize,
                             final int coreThreadCount,
                             final int maxThreadCount,
                             final boolean threadsReadResults,
                             final int completionQueueSize) {
        super(name, maxExecutorPoolSize);
        this.coreThreadCount = coreThreadCount;
        this.maxThreadCount = maxThreadCount;
        this.threadsReadResults = threadsReadResults;
        this.completionQueueSize = completionQueueSize;
    }

    public int getCoreThreadCount() {
        return this.coreThreadCount;
    }

    public int getMaxThreadCount() {
        return this.maxThreadCount;
    }

    public boolean getThreadsReadResults() {
        return this.threadsReadResults;
    }

    public int getCompletionQueueSize() {
        return this.completionQueueSize;
    }

    protected CompletionQueueExecutor newElement() throws HBqlException {
        return this.getThreadsReadResults()
               ? ResultExecutor.newPooledResultExecutor(this,
                                                        this.getCoreThreadCount(),
                                                        this.getMaxThreadCount(),
                                                        this.getCompletionQueueSize())
               : ResultScannerExecutor.newPooledResultScannerExecutor(this,
                                                                      this.getCoreThreadCount(),
                                                                      this.getMaxThreadCount(),
                                                                      this.getCompletionQueueSize());
    }
}
