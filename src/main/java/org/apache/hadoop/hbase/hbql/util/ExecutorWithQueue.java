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

package org.apache.hadoop.hbase.hbql.util;

import org.apache.hadoop.hbase.hbql.client.QueryExecutorPool;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ExecutorWithQueue<T> implements PoolableElement {

    private final QueryExecutorPool executorPool;
    private final ExecutorService threadPool;
    private final QueueWithCompletion<T> completionQueue;
    private final AtomicInteger workSubmittedCount = new AtomicInteger(0);

    protected ExecutorWithQueue(final QueryExecutorPool executorPool, final int threadCount, final int queueSize) {
        this.executorPool = executorPool;
        this.threadPool = Executors.newFixedThreadPool(threadCount);
        this.completionQueue = new QueueWithCompletion<T>(queueSize);
    }

    public abstract boolean threadsReadResults();

    protected QueryExecutorPool getExecutorPool() {
        return this.executorPool;
    }

    protected ExecutorService getThreadPool() {
        return this.threadPool;
    }

    public QueueWithCompletion<T> getCompletionQueue() {
        return this.completionQueue;
    }

    protected AtomicInteger getWorkSubmittedCount() {
        return this.workSubmittedCount;
    }

    public boolean moreResultsPending(final int val) {
        return val < this.getWorkSubmittedCount().get();
    }

    public void reset() {
        this.getWorkSubmittedCount().set(0);
        this.getCompletionQueue().reset();
    }

    public Future<String> submit(final Callable<String> job) {
        final Future<String> future = this.getThreadPool().submit(job);
        this.getWorkSubmittedCount().incrementAndGet();
        return future;
    }

    public boolean isPooled() {
        return this.getExecutorPool() != null;
    }

    public void release() {
        // Release if it is a pool element
        if (this.isPooled())
            this.getExecutorPool().release(this);
    }
}
