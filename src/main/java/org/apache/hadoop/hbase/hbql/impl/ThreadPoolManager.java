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

import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.Map;

public class ThreadPoolManager {

    private static Map<String, ThreadPool> threadPoolMap = Maps.newConcurrentHashMap();

    public static ThreadPool newThreadPool(final String threadPoolName,
                                           final int maxThreadPoolSize,
                                           final int numberOfThreads) throws HBqlException {

        final ThreadPool threadPool = new ThreadPool(threadPoolName, maxThreadPoolSize, numberOfThreads);

        if (threadPool.getName() != null && threadPool.getName().length() > 0) {
            if (getThreadPoolMap().containsKey(threadPool.getName()))
                throw new HBqlException("Thread pool already exists: " + threadPool.getName());

            getThreadPoolMap().put(threadPool.getName(), threadPool);
        }

        return threadPool;
    }

    public static ThreadPool getThreadPool(final String name) {
        return ThreadPoolManager.getThreadPoolMap().get(name);
    }

    private static Map<String, ThreadPool> getThreadPoolMap() {
        return ThreadPoolManager.threadPoolMap;
    }
}
