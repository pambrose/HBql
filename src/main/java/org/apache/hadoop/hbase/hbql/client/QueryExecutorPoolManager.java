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

package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.util.Maps;

import java.util.Collection;
import java.util.Map;

public class QueryExecutorPoolManager {

    public static int defaultMaxExecutorPoolSize = 5;
    public static int defaultMinThreadCount = 1;
    public static int defaultMaxThreadCount = 10;
    public static long defaultKeepAliveSecs = Long.MAX_VALUE;
    public static boolean defaultThreadsReadResults = true;
    public static int defaultCompletionQueueSize = 25;

    private static Map<String, QueryExecutorPool> executorPoolMap = Maps.newConcurrentHashMap();

    private static Map<String, QueryExecutorPool> getExecutorPoolMap() {
        return QueryExecutorPoolManager.executorPoolMap;
    }

    public static QueryExecutorPool newQueryExecutorPool(final String poolName,
                                                         final int maxExecutorPoolSize,
                                                         final int minThreadCount,
                                                         final int maxThreadCount,
                                                         final long keepAliveSecs,
                                                         final boolean threadsReadResults,
                                                         final int completionQueueSize) throws HBqlException {

        if (Utils.isValidString(poolName) && getExecutorPoolMap().containsKey(poolName))
            throw new HBqlException("Executor pool already exists: " + poolName);

        final QueryExecutorPool executorPool = new QueryExecutorPool(poolName,
                                                                     maxExecutorPoolSize,
                                                                     minThreadCount,
                                                                     maxThreadCount,
                                                                     keepAliveSecs,
                                                                     threadsReadResults,
                                                                     completionQueueSize);
        QueryExecutorPoolManager.getExecutorPoolMap().put(executorPool.getName(), executorPool);

        return executorPool;
    }

    public static boolean dropExecutorPool(final String name) {

        if (Utils.isValidString(name) && getExecutorPoolMap().containsKey(name)) {
            getExecutorPoolMap().remove(name);
            return true;
        }

        return false;
    }

    public static boolean queryExecutorPoolExists(final String name) {
        return Utils.isValidString(name) && getExecutorPoolMap().containsKey(name);
    }

    public static QueryExecutorPool getExecutorPool(final String poolName) throws HBqlException {
        if (!QueryExecutorPoolManager.getExecutorPoolMap().containsKey(poolName))
            throw new HBqlException("Missing executor pool: " + poolName);

        return QueryExecutorPoolManager.getExecutorPoolMap().get(poolName);
    }

    public static Collection<QueryExecutorPool> getQueryExecutorPools() {
        return getExecutorPoolMap().values();
    }
}
