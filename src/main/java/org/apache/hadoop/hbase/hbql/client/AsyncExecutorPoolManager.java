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

package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.util.Maps;

import java.util.Collection;
import java.util.Map;

public class AsyncExecutorPoolManager {

    public static int defaultMaxExecutorPoolSize = 5;
    public static int defaultMinThreadCount = 1;
    public static int defaultMaxThreadCount = 10;
    public static long defaultKeepAliveSecs = Long.MAX_VALUE;

    private static Map<String, AsyncExecutorPool> executorPoolMap = Maps.newConcurrentHashMap();

    private static Map<String, AsyncExecutorPool> getExecutorPoolMap() {
        return AsyncExecutorPoolManager.executorPoolMap;
    }

    public static AsyncExecutorPool newAsyncExecutorPool(final String poolName,
                                                         final int maxExecutorPoolSize,
                                                         final int minThreadCount,
                                                         final int maxThreadCount,
                                                         final long keepAliveSecs) throws HBqlException {

        if (Utils.isValidString(poolName) && getExecutorPoolMap().containsKey(poolName))
            throw new HBqlException("AsyncExecutorPool already exists: " + poolName);

        final AsyncExecutorPool executorPool = new AsyncExecutorPool(poolName,
                                                                     maxExecutorPoolSize,
                                                                     minThreadCount,
                                                                     maxThreadCount,
                                                                     keepAliveSecs);
        AsyncExecutorPoolManager.getExecutorPoolMap().put(executorPool.getName(), executorPool);

        return executorPool;
    }

    public static boolean dropAsyncExecutorPool(final String name) {

        if (Utils.isValidString(name) && getExecutorPoolMap().containsKey(name)) {
            final AsyncExecutorPool asyncExecutorPool = getExecutorPoolMap().remove(name);
            asyncExecutorPool.shutdown();
            return true;
        }

        return false;
    }

    public static boolean asyncExecutorPoolExists(final String name) {
        return Utils.isValidString(name) && getExecutorPoolMap().containsKey(name);
    }

    public static AsyncExecutorPool getExecutorPool(final String poolName) throws HBqlException {
        if (!AsyncExecutorPoolManager.getExecutorPoolMap().containsKey(poolName))
            throw new HBqlException("AsyncExecutorPool does not exist: " + poolName);

        return AsyncExecutorPoolManager.getExecutorPoolMap().get(poolName);
    }

    public static Collection<AsyncExecutorPool> getAsyncExecutorPools() {
        return getExecutorPoolMap().values();
    }
}