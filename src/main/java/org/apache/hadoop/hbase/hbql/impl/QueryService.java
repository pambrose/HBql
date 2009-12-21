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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class QueryService implements PoolableElement {

    private final BlockingQueue<Future<ResultScanner>> futureQueue = new LinkedBlockingQueue<Future<ResultScanner>>();
    private final List<Future<ResultScanner>> futureList = Lists.newArrayList();
    private final QueryServicePool queryServicePool;
    private final ExecutorService executorService;
    private final ExecutorCompletionService<ResultScanner> executorCompletionService;

    public QueryService(final QueryServicePool queryServicePool, final int numberOfThreads) {
        this.queryServicePool = queryServicePool;
        this.executorService = Executors.newFixedThreadPool(numberOfThreads);
        this.executorCompletionService = new ExecutorCompletionService<ResultScanner>(this.getExecutorService(),
                                                                                      this.getFutureQueue());
    }

    private QueryServicePool getThreadPool() {
        return this.queryServicePool;
    }

    private ExecutorService getExecutorService() {
        return this.executorService;
    }

    private List<Future<ResultScanner>> getFutureList() {
        return this.futureList;
    }

    private BlockingQueue<Future<ResultScanner>> getFutureQueue() {
        return this.futureQueue;
    }

    private ExecutorCompletionService<ResultScanner> getExecutorCompletionService() {
        return this.executorCompletionService;
    }

    public Future<ResultScanner> submit(final Callable<ResultScanner> job) {
        final Future<ResultScanner> future = this.executorCompletionService.submit(job);
        this.getFutureList().add(future);
        return future;
    }

    public Future<ResultScanner> take() throws InterruptedException {
        return this.getExecutorCompletionService().take();
    }

    public boolean moreResultsPending() {

        // See if results are waiting to be processed
        if (this.getFutureQueue().size() > 0)
            return true;

        // See if work is still taking place.
        for (final Future<ResultScanner> future : this.getFutureList())
            if (!future.isDone())
                return true;

        return false;
    }

    public void reset() {
        for (final Future<ResultScanner> future : getFutureList()) {
            if (!future.isDone())
                future.cancel(true);
        }

        this.getFutureList().clear();
        this.getFutureQueue().clear();
    }

    public void release() {
        this.getThreadPool().release(this);
    }
}
