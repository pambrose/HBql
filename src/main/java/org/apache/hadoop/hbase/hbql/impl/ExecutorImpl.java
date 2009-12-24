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
import org.apache.hadoop.hbase.hbql.client.Executor;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class ExecutorImpl<T> extends Executor implements HExecutor {

    private final BlockingQueue<Future<T>> backingQueue = new LinkedBlockingQueue<Future<T>>();
    private final List<Future<T>> futureList = Lists.newArrayList();
    private final ExecutorPool executorPool;
    private final ExecutorService executorService;
    private final ExecutorCompletionService<T> completionService;

    public ExecutorImpl(final ExecutorPool executorPool, final int threadCount) {
        this.executorPool = executorPool;
        this.executorService = Executors.newFixedThreadPool(threadCount);
        this.completionService = new ExecutorCompletionService<T>(this.getExecutorService(), this.getBackingQueue());
    }

    public static ExecutorImpl newExecutorForPool(final ExecutorPool executorPool, final int threadCount) {
        return new ExecutorImpl(executorPool, threadCount);
    }

    private ExecutorPool getExecutorPool() {
        return this.executorPool;
    }

    public ExecutorService getExecutorService() {
        return this.executorService;
    }

    private List<Future<T>> getFutureList() {
        return this.futureList;
    }

    private BlockingQueue<Future<T>> getBackingQueue() {
        return this.backingQueue;
    }

    private ExecutorCompletionService<T> getCompletionService() {
        return this.completionService;
    }

    public Future<T> submit(final Callable<T> job) {
        final Future<T> future = this.getCompletionService().submit(job);
        this.getFutureList().add(future);
        return future;
    }

    public T takeResultScanner() throws HBqlException {
        try {
            final Future<T> future = this.getCompletionService().take();
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
        if (this.getBackingQueue().size() > 0) {
            System.out.println("Backing queue size:  " + this.getBackingQueue().size());
            return true;
        }

        // See if work is still taking place.
        return this.hasThreadsStillRunning();
    }

    public boolean hasThreadsStillRunning() {
        for (final Future<T> future : this.getFutureList())
            if (!future.isDone()) {
                System.out.println("a thread is not done");
                return true;
            }

        System.out.println("all threads done");
        return false;
    }

    public void reset() {
        for (final Future<T> future : getFutureList()) {
            if (!future.isDone())
                future.cancel(true);
        }

        this.getFutureList().clear();
        this.getBackingQueue().clear();
    }

    public void release() {
        // Release if it is a pool element
        if (this.getExecutorPool() != null)
            this.getExecutorPool().release(this);
    }
}
