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

package org.apache.expreval.util;

import org.apache.hadoop.hbase.hbql.client.QueryExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class GenericExecutor<T> extends QueryExecutor implements PoolableElement {

    private final ExecutorPool executorPool;
    private final ExecutorService threadPool;
    private final BlockingQueueWithCompletion<T> queue;
    private final AtomicInteger workSubmittedCount = new AtomicInteger(0);

    protected GenericExecutor(final ExecutorPool executorPool, final int threadCount, final int queueSize) {
        this.executorPool = executorPool;
        this.threadPool = Executors.newFixedThreadPool(threadCount);
        this.queue = new BlockingQueueWithCompletion<T>(queueSize);
    }

    public abstract boolean threadsReadResults();

    protected ExecutorPool getExecutorPool() {
        return this.executorPool;
    }

    protected ExecutorService getThreadPool() {
        return this.threadPool;
    }

    public BlockingQueueWithCompletion<T> getQueue() {
        return this.queue;
    }

    protected AtomicInteger getWorkSubmittedCount() {
        return this.workSubmittedCount;
    }

    public void reset() {
        this.getWorkSubmittedCount().set(0);
        this.getQueue().reset();
    }

    public Future<String> submit(final Callable<String> job) {
        final Future<String> future = this.getThreadPool().submit(job);
        this.getWorkSubmittedCount().incrementAndGet();
        return future;
    }

    public boolean moreResultsPending(final int val) {
        return val < this.getWorkSubmittedCount().get();
    }

    public void release() {
        // Release if it is a pool element
        if (this.getExecutorPool() != null)
            this.getExecutorPool().release(this);
    }
}
