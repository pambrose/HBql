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

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPool {

    private final String name;
    private final int maxThreadPoolSize;
    private final int numberOfThreads;
    private volatile int count = 0;

    private final BlockingQueue<ExecutorService> executorPool;

    public ThreadPool(final String name, final int maxThreadPoolSize, final int numberOfThreads) {
        this.name = name;
        this.maxThreadPoolSize = maxThreadPoolSize;
        this.numberOfThreads = numberOfThreads;

        this.executorPool = new ArrayBlockingQueue<ExecutorService>(this.getMaxThreadPoolSize());
    }

    public String getName() {
        return name;
    }

    private BlockingQueue<ExecutorService> getExecutorPool() {
        return this.executorPool;
    }

    private int getMaxThreadPoolSize() {
        return maxThreadPoolSize;
    }

    private int getNumberOfThreads() {
        return numberOfThreads;
    }

    private int getCount() {
        return this.count;
    }

    private void addExecutorToPool() throws HBqlException {
        if (this.getCount() < this.getMaxThreadPoolSize()) {
            final ExecutorService executor = Executors.newFixedThreadPool(this.getNumberOfThreads());
            this.getExecutorPool().add(executor);
            this.count++;
        }
    }

    public synchronized Executor getExecutor() throws HBqlException {

        //  Grow the pool as necessary, rather than front-loading it.
        if (this.getExecutorPool().size() == 0)
            this.addExecutorToPool();

        try {
            return this.getExecutorPool().take();
        }
        catch (InterruptedException e) {
            throw new HBqlException("InterruptedException: " + e.getMessage());
        }
    }

    public void release(final ExecutorService executor) {
        this.getExecutorPool().add(executor);
    }
}
