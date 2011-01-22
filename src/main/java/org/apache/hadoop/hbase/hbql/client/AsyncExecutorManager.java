/*
 * Copyright (c) 2011.  The Apache Software Foundation
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

import org.apache.hadoop.hbase.hbql.impl.AsyncExecutorImpl;
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.util.Maps;

import java.util.Map;
import java.util.Set;

public class AsyncExecutorManager {

    public static int  defaultMinThreadCount = 1;
    public static int  defaultMaxThreadCount = 10;
    public static long defaultKeepAliveSecs  = Long.MAX_VALUE;

    private static Map<String, AsyncExecutorImpl> executorMap = Maps.newConcurrentHashMap();

    private static Map<String, AsyncExecutorImpl> getExecutorMap() {
        return AsyncExecutorManager.executorMap;
    }

    public static AsyncExecutor newAsyncExecutor(final String name,
                                                 final int minThreadCount,
                                                 final int maxThreadCount,
                                                 final long keepAliveSecs) throws HBqlException {

        if (Utils.isValidString(name) && getExecutorMap().containsKey(name))
            throw new HBqlException("AsyncExecutor already exists: " + name);

        final AsyncExecutorImpl asyncExecutor = new AsyncExecutorImpl(name,
                                                                      minThreadCount,
                                                                      maxThreadCount,
                                                                      keepAliveSecs);
        AsyncExecutorManager.getExecutorMap().put(asyncExecutor.getName(), asyncExecutor);

        return asyncExecutor;
    }

    public static boolean dropAsyncExecutor(final String name) {

        if (Utils.isValidString(name) && getExecutorMap().containsKey(name)) {
            final AsyncExecutorImpl asyncExecutor = getExecutorMap().remove(name);
            asyncExecutor.shutdown();
            return true;
        }

        return false;
    }

    public static boolean asyncExecutorExists(final String name) {
        return Utils.isValidString(name) && AsyncExecutorManager.getExecutorMap().containsKey(name);
    }

    public static AsyncExecutor getAsyncExecutor(final String name) throws HBqlException {
        if (!AsyncExecutorManager.getExecutorMap().containsKey(name))
            throw new HBqlException("AsyncExecutor does not exist: " + name);

        return AsyncExecutorManager.getExecutorMap().get(name);
    }

    public static Set<String> getAsyncExecutorNames() {
        return AsyncExecutorManager.getExecutorMap().keySet();
    }
}