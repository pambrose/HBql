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

import org.apache.expreval.util.ExecutorPool;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.impl.Utils;

import java.util.Collection;
import java.util.Map;

public class HExecutorPoolManager {

    private static Map<String, ExecutorPool> executorPoolMap = Maps.newConcurrentHashMap();

    private static Map<String, ExecutorPool> getExecutorPoolMap() {
        return HExecutorPoolManager.executorPoolMap;
    }

    public static ExecutorPool newExecutorPool(final String poolName,
                                               final int maxPoolSize,
                                               final int threadCount,
                                               final boolean threadsReadResults,
                                               final int queueSize) throws HBqlException {

        if (Utils.isValidString(poolName) && getExecutorPoolMap().containsKey(poolName))
            throw new HBqlException("Executor pool already exists: " + poolName);

        final ExecutorPool executorPool = new ExecutorPool(poolName,
                                                           maxPoolSize,
                                                           threadCount,
                                                           threadsReadResults,
                                                           queueSize);
        getExecutorPoolMap().put(executorPool.getName(), executorPool);

        return executorPool;
    }

    public static boolean dropExecutorPool(final String name) {

        if (Utils.isValidString(name) && getExecutorPoolMap().containsKey(name)) {
            getExecutorPoolMap().remove(name);
            return true;
        }

        return false;
    }

    public static boolean executorPoolExists(final String name) {
        return Utils.isValidString(name) && getExecutorPoolMap().containsKey(name);
    }

    public static ExecutorPool getExecutorPool(final String poolName) throws HBqlException {
        if (!HExecutorPoolManager.getExecutorPoolMap().containsKey(poolName))
            throw new HBqlException("Missing executor pool: " + poolName);

        return HExecutorPoolManager.getExecutorPoolMap().get(poolName);
    }

    public static Collection<ExecutorPool> getExecutorPools() {
        return getExecutorPoolMap().values();
    }
}
