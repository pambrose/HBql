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

    private static Map<String, QueryServicePool> threadPoolMap = Maps.newConcurrentHashMap();

    public static QueryServicePool newThreadPool(final String threadPoolName,
                                                 final int maxPoolSize,
                                                 final int numberOfThreads) throws HBqlException {

        final QueryServicePool queryServicePool = new QueryServicePool(threadPoolName, maxPoolSize, numberOfThreads);

        if (queryServicePool.getName() != null && queryServicePool.getName().length() > 0) {
            if (getThreadPoolMap().containsKey(queryServicePool.getName()))
                throw new HBqlException("Thread pool already exists: " + queryServicePool.getName());

            getThreadPoolMap().put(queryServicePool.getName(), queryServicePool);
        }

        return queryServicePool;
    }

    public static boolean dropThreadPool(final String name) {

        if (name != null && name.length() > 0) {
            if (getThreadPoolMap().containsKey(name)) {
                getThreadPoolMap().remove(name);
                return true;
            }
        }

        return false;
    }

    private static Map<String, QueryServicePool> getThreadPoolMap() {
        return ThreadPoolManager.threadPoolMap;
    }

    public static QueryServicePool getThreadPool(final String poolName) throws HBqlException {
        if (!ThreadPoolManager.getThreadPoolMap().containsKey(poolName))
            throw new HBqlException("Missing thread pool: " + poolName);

        return ThreadPoolManager.getThreadPoolMap().get(poolName);
    }
}
