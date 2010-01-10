/*
 * Copyright (c) 2010.  The Apache Software Foundation
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

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.QueryFuture;
import org.apache.hadoop.hbase.hbql.util.NamedThreadFactory;
import org.apache.hadoop.hbase.hbql.util.PoolableElement;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class UnboundedAsyncExecutor extends PoolableElement<UnboundedAsyncExecutor> {

    private final AtomicBoolean atomicShutdown = new AtomicBoolean(false);
    private final AtomicInteger workSubmittedCounter = new AtomicInteger(0);
    private final LocalThreadPoolExecutor threadPoolExecutor;

    private static class LocalThreadPoolExecutor extends ThreadPoolExecutor {

        private final AtomicInteger queryCounter = new AtomicInteger(0);

        private LocalThreadPoolExecutor(final int minPoolSize,
                                        final int maxPoolSize,
                                        final long keepAliveTime,
                                        final TimeUnit timeUnit,
                                        final BlockingQueue<Runnable> backingQueue,
                                        final ThreadFactory threadFactory) {
            super(minPoolSize, maxPoolSize, keepAliveTime, timeUnit, backingQueue, threadFactory);
        }

        private AtomicInteger getQueryCounter() {
            return this.queryCounter;
        }

        private void incrementQueryCount() {
            this.getQueryCounter().incrementAndGet();
        }

        private void reset() {
            this.getQueryCounter().set(0);
        }

        public int getQueryCount() {
            return this.getQueryCounter().get();
        }

        protected void beforeExecute(final Thread thread, final Runnable runnable) {
            super.beforeExecute(thread, runnable);

            final AsyncRunnable asyncRunnable = (AsyncRunnable)runnable;
            asyncRunnable.getQueryFuture().markQueryStart();
        }

        protected void afterExecute(final Runnable runnable, final Throwable throwable) {
            super.afterExecute(runnable, throwable);

            final AsyncRunnable asyncRunnable = (AsyncRunnable)runnable;
            asyncRunnable.getQueryFuture().markQueryComplete();
        }
    }

    public UnboundedAsyncExecutor(final AsyncExecutorPoolImpl executorPool,
                                  final int minThreadCount,
                                  final int maxThreadCount,
                                  final long keepAliveSecs) throws HBqlException {
        super(executorPool);
        final BlockingQueue<Runnable> backingQueue = new LinkedBlockingQueue<Runnable>();
        final String name = executorPool == null ? "Non async exec pool" : "Async exec pool " + executorPool.getName();
        this.threadPoolExecutor = new LocalThreadPoolExecutor(minThreadCount,
                                                              maxThreadCount,
                                                              keepAliveSecs,
                                                              TimeUnit.SECONDS,
                                                              backingQueue,
                                                              new NamedThreadFactory(name));
    }

    private LocalThreadPoolExecutor getThreadPoolExecutor() {
        return this.threadPoolExecutor;
    }

    private AtomicInteger getWorkSubmittedCounter() {
        return this.workSubmittedCounter;
    }

    public void resetElement() {
        this.getWorkSubmittedCounter().set(0);
        this.getThreadPoolExecutor().reset();
    }

    public QueryFuture submit(final AsyncRunnable job) {
        this.getWorkSubmittedCounter().incrementAndGet();
        this.getThreadPoolExecutor().execute(job);
        return job.getQueryFuture();
    }

    public void close() {
        this.resetElement();
        this.releaseElement();
    }

    public void releaseElement() {
        this.getElementPool().release(this);
    }

    private AtomicBoolean getAtomicShutdown() {
        return this.atomicShutdown;
    }

    public boolean isShutdown() {
        return this.getAtomicShutdown().get();
    }

    public void shutdown() {
        if (!this.isShutdown()) {
            synchronized (this) {
                if (!this.isShutdown()) {
                    this.getThreadPoolExecutor().shutdown();
                    this.getAtomicShutdown().set(true);
                }
            }
        }
    }
}