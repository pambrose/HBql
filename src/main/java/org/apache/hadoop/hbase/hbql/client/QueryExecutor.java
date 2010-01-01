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

import org.apache.hadoop.hbase.hbql.impl.QueryExecutorImpl;
import org.apache.hadoop.hbase.hbql.impl.ResultExecutor;
import org.apache.hadoop.hbase.hbql.impl.ResultScannerExecutor;

public class QueryExecutor {

    public static QueryExecutor newQueryExecutor(final int minThreadCount,
                                                 final int maxThreadCount,
                                                 final long keepAliveSecs,
                                                 final boolean threadsReadResults,
                                                 final int queueSize) {
        return new QueryExecutorImpl(threadsReadResults
                                     ? ResultExecutor.newResultExecutor(minThreadCount,
                                                                        maxThreadCount,
                                                                        keepAliveSecs,
                                                                        queueSize)
                                     : ResultScannerExecutor.newResultScannerExecutor(minThreadCount,
                                                                                      maxThreadCount,
                                                                                      keepAliveSecs,
                                                                                      queueSize));
    }

    private QueryExecutorImpl getExecutorImpl() {
        return (QueryExecutorImpl)this;
    }

    public boolean isPooled() {
        return this.getExecutorImpl().getExecutor().isPooled();
    }
}
