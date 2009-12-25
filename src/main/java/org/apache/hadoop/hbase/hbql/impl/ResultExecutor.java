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

import org.apache.expreval.util.BlockingQueueWithCompletion;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.Executor;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class ResultExecutor extends Executor implements HExecutor {

    private final AtomicInteger workSubmittedCount = new AtomicInteger(0);
    private final BlockingQueueWithCompletion<Result> resultQueue;

    private ResultExecutor(final ExecutorPool executorPool, final int threadCount, final int queueSize) {
        super(executorPool, threadCount);

        // TODO need the user to specify buffer size
        this.resultQueue = new BlockingQueueWithCompletion<Result>(queueSize);
    }

    public static ResultExecutor newPooledResultExecutor(final ExecutorPool executorPool,
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

    public BlockingQueueWithCompletion<Result> getResultQueue() {
        return this.resultQueue;
    }

    private AtomicInteger getWorkSubmittedCount() {
        return this.workSubmittedCount;
    }

    public Future<String> submit(final Callable<String> job) {
        final Future<String> future = this.getExecutorService().submit(job);
        this.getWorkSubmittedCount().incrementAndGet();
        return future;
    }

    public boolean moreResultsPending(final int val) {
        return val < this.getWorkSubmittedCount().get();
    }

    public void reset() {
        this.getWorkSubmittedCount().set(0);
        this.getResultQueue().reset();
    }

    public void release() {
        // Release if it is a pool element
        if (this.getExecutorPool() != null)
            this.getExecutorPool().release(this);
    }
}