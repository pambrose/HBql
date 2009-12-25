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

import org.apache.hadoop.hbase.hbql.impl.ExecutorPool;
import org.apache.hadoop.hbase.hbql.impl.ResultExecutor;
import org.apache.hadoop.hbase.hbql.impl.ResultScannerExecutor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Executor {

    private final ExecutorPool executorPool;
    private final ExecutorService executorService;

    protected Executor(final ExecutorPool executorPool, final int threadCount) {
        this.executorPool = executorPool;
        this.executorService = Executors.newFixedThreadPool(threadCount);
    }

    public static Executor newExecutor(final int threadCount, final boolean threadsReadResults) {
        return threadsReadResults
               ? ResultExecutor.newResultExecutor(threadCount)
               : ResultScannerExecutor.newResultScannerExecutor(threadCount);
    }

    protected ExecutorPool getExecutorPool() {
        return this.executorPool;
    }

    protected ExecutorService getExecutorService() {
        return this.executorService;
    }
}
