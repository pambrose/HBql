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

package org.apache.hadoop.hbase.hbql.util;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.QueryExecutorPool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class CompletionQueueExecutor<T> implements PoolableElement {

    private final AtomicBoolean atomicShutdown = new AtomicBoolean(false);
    private final AtomicInteger workSubmittedCounter = new AtomicInteger(0);
    private final ExecutorService submitterThread = Executors.newSingleThreadExecutor();
    private final QueryExecutorPool executorPool;
    private final LocalThreadPoolExecutor threadPoolExecutor;
    private final CompletionQueue<T> completionQueue;

    private static class LocalCallerRunsPolicy extends ThreadPoolExecutor.CallerRunsPolicy {

        public void rejectedExecution(final Runnable runnable, final ThreadPoolExecutor threadPoolExecutor) {
            super.rejectedExecution(runnable, threadPoolExecutor);
            ((LocalThreadPoolExecutor)threadPoolExecutor).incrementRejectionCount();
        }
    }

    private static class LocalThreadPoolExecutor extends ThreadPoolExecutor {

        private final AtomicInteger rejectionCounter = new AtomicInteger(0);

        private LocalThreadPoolExecutor(final int minPoolSize,
                                        final int maxPoolSize,
                                        final long keepAliveTime,
                                        final TimeUnit timeUnit,
                                        final BlockingQueue<Runnable> workQueue,
                                        final ThreadFactory threadFactory,
                                        final RejectedExecutionHandler handler) {
            super(minPoolSize, maxPoolSize, keepAliveTime, timeUnit, workQueue, threadFactory, handler);
        }

        private AtomicInteger getRejectionCounter() {
            return this.rejectionCounter;
        }

        private void incrementRejectionCount() {
            this.getRejectionCounter().incrementAndGet();
        }

        private void reset() {
            this.getRejectionCounter().set(0);
        }

        public int getRejectionCount() {
            return this.getRejectionCounter().get();
        }
    }

    private static class LocalThreadFactory implements ThreadFactory {

        private final AtomicInteger threadCounter = new AtomicInteger(0);
        private final String name;

        private LocalThreadFactory(final String name) {
            this.name = name;
        }

        public Thread newThread(Runnable r) {
            final Thread thread = new Thread(r);
            thread.setName(this.name + " thread: " + this.threadCounter.incrementAndGet());
            return thread;
        }
    }

    protected CompletionQueueExecutor(final QueryExecutorPool executorPool,
                                      final int minThreadCount,
                                      final int maxThreadCount,
                                      final long keepAliveSecs,
                                      final int completionQueueSize) throws HBqlException {
        this.executorPool = executorPool;
        final BlockingQueue<Runnable> backingQueue = new ArrayBlockingQueue<Runnable>(maxThreadCount * 5);
        final String name = executorPool == null ? "Non pool" : "Executor pool " + executorPool.getName();
        this.threadPoolExecutor = new LocalThreadPoolExecutor(minThreadCount,
                                                              maxThreadCount,
                                                              keepAliveSecs,
                                                              TimeUnit.SECONDS,
                                                              backingQueue,
                                                              new LocalThreadFactory(name),
                                                              new LocalCallerRunsPolicy());
        this.completionQueue = new CompletionQueue<T>(completionQueueSize);
    }

    public abstract boolean threadsReadResults();

    private QueryExecutorPool getExecutorPool() {
        return this.executorPool;
    }

    private ExecutorService getSubmitterThread() {
        return this.submitterThread;
    }

    private LocalThreadPoolExecutor getThreadPoolExecutor() {
        return this.threadPoolExecutor;
    }

    private CompletionQueue<T> getCompletionQueue() {
        return this.completionQueue;
    }

    private AtomicInteger getWorkSubmittedCounter() {
        return this.workSubmittedCounter;
    }

    public void putElement(final T val) throws HBqlException {
        this.getCompletionQueue().putElement(val);
    }

    public void putCompletion() {
        this.getCompletionQueue().putCompletionToken();
    }

    public CompletionQueue.Element<T> takeElement() throws HBqlException {
        return this.getCompletionQueue().takeElement();
    }

    public int getRejectionCount() {
        return this.getThreadPoolExecutor().getRejectionCount();
    }

    public boolean moreResultsPending() {
        final int completionCount = this.getCompletionQueue().getCompletionCount();
        final int submittedCount = this.getWorkSubmittedCounter().get();
        return completionCount < submittedCount;
    }

    public void reset() {
        this.getWorkSubmittedCounter().set(0);
        this.getCompletionQueue().reset();
        this.getThreadPoolExecutor().reset();
    }

    public void submitWorkToSubmitterThread(final Runnable job) {
        this.getWorkSubmittedCounter().incrementAndGet();
        this.getSubmitterThread().submit(job);
    }

    public void submitWorkToThreadPool(final Runnable job) {
        this.getWorkSubmittedCounter().incrementAndGet();
        this.getThreadPoolExecutor().execute(job);
    }

    public boolean isPooled() {
        return this.getExecutorPool() != null;
    }

    public void release() {
        // Release if it is a pool element
        if (this.isPooled())
            this.getExecutorPool().release(this);
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
