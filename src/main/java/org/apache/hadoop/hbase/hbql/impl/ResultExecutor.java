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

import org.apache.hadoop.hbase.hbql.client.Executor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class ResultExecutor extends Executor implements HExecutor {

    private final ExecutorPool executorPool;
    private final ExecutorService executorService;

    private final AtomicInteger workCount = new AtomicInteger(0);

    private ResultExecutor(final ExecutorPool executorPool, final int threadCount) {
        this.executorPool = executorPool;
        this.executorService = Executors.newFixedThreadPool(threadCount);
    }

    public static ResultExecutor newExecutorForPool(final ExecutorPool executorPool, final int threadCount) {
        return new ResultExecutor(executorPool, threadCount);
    }

    public static ResultExecutor newExecutorNotForPool(final int threadCount) {
        return new ResultExecutor(null, threadCount);
    }

    private ExecutorPool getExecutorPool() {
        return this.executorPool;
    }

    private AtomicInteger getWorkCount() {
        return this.workCount;
    }

    public ExecutorService getExecutorService() {
        return this.executorService;
    }

    public Future<String> submit(final Callable<String> job) {
        final Future<String> future = this.getExecutorService().submit(job);
        this.getWorkCount().incrementAndGet();
        return future;
    }

    public boolean moreResultsPending(final int val) {
        // System.out.println("Comparing: " + val + " and " + this.workCount);
        return val < this.getWorkCount().get();
    }

    public void reset() {
        this.getWorkCount().set(0);
    }

    public void release() {
        // Release if it is a pool element
        if (this.getExecutorPool() != null)
            this.getExecutorPool().release(this);
    }
}