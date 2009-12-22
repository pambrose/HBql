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

import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class Executor implements PoolableElement {

    private final BlockingQueue<Future<ResultScanner>> futureQueue = new LinkedBlockingQueue<Future<ResultScanner>>();
    private final List<Future<ResultScanner>> futureList = Lists.newArrayList();
    private final ExecutorPool executorPool;
    private final ExecutorService executorService;
    private final ExecutorCompletionService<ResultScanner> executorCompletionService;

    private Executor(final ExecutorPool executorPool, final int threadCount) {
        this.executorPool = executorPool;
        this.executorService = Executors.newFixedThreadPool(threadCount);
        this.executorCompletionService = new ExecutorCompletionService<ResultScanner>(this.getExecutorService(),
                                                                                      this.getExecutorBackingQueue());
    }

    public static Executor newExecutorForPool(final ExecutorPool executorPool, final int threadCount) {
        return new Executor(executorPool, threadCount);
    }

    public static Executor newExecutor(final int threadCount) {
        return Executor.newExecutorForPool(null, threadCount);
    }

    private ExecutorPool getExecutorPool() {
        return this.executorPool;
    }

    private ExecutorService getExecutorService() {
        return this.executorService;
    }

    private List<Future<ResultScanner>> getFutureList() {
        return this.futureList;
    }

    private BlockingQueue<Future<ResultScanner>> getExecutorBackingQueue() {
        return this.futureQueue;
    }

    private ExecutorCompletionService<ResultScanner> getExecutorCompletionService() {
        return this.executorCompletionService;
    }

    public Future<ResultScanner> submit(final Callable<ResultScanner> job) {
        final Future<ResultScanner> future = this.getExecutorCompletionService().submit(job);
        this.getFutureList().add(future);
        return future;
    }

    public ResultScanner takeResultScanner() throws HBqlException {
        try {
            final Future<ResultScanner> future = this.getExecutorCompletionService().take();
            return future.get();
        }
        catch (InterruptedException e) {
            throw new HBqlException(e);
        }
        catch (java.util.concurrent.ExecutionException e) {
            throw new HBqlException(e);
        }
    }

    public boolean moreResultsPending() {

        // See if results are waiting to be processed
        if (this.getExecutorBackingQueue().size() > 0) {
            return true;
        }

        // See if work is still taking place.
        for (final Future<ResultScanner> future : this.getFutureList())
            if (!future.isDone()) {
                return true;
            }

        return false;
    }

    public void reset() {
        for (final Future<ResultScanner> future : getFutureList()) {
            if (!future.isDone())
                future.cancel(true);
        }

        this.getFutureList().clear();
        this.getExecutorBackingQueue().clear();
    }

    public void release() {
        // Release if it is a pool element
        if (this.getExecutorPool() != null)
            this.getExecutorPool().release(this);
    }
}
