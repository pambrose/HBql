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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.hbql.impl.HConnectionPoolImpl;
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.util.Maps;

import java.util.Map;

public class HConnectionPoolManager {

    private static Map<String, HConnectionPool> connectionPoolMap = Maps.newConcurrentHashMap();

    private static int maxPoolReferencesPerTablePerConnection = Integer.MAX_VALUE;

    public static HConnectionPool newConnectionPool(final int initialPoolSize,
                                                    final int maxPoolSize) throws HBqlException {
        return HConnectionPoolManager.newConnectionPool(initialPoolSize,
                                                        maxPoolSize,
                                                        null,
                                                        null);
    }

    public static HConnectionPool newConnectionPool(final int initialPoolSize,
                                                    final int maxPoolSize,
                                                    final HBaseConfiguration config) throws HBqlException {
        return HConnectionPoolManager.newConnectionPool(initialPoolSize,
                                                        maxPoolSize,
                                                        null,
                                                        config);
    }

    public static HConnectionPool newConnectionPool(final int initialPoolSize,
                                                    final int maxPoolSize,
                                                    final String poolName) throws HBqlException {
        return HConnectionPoolManager.newConnectionPool(initialPoolSize,
                                                        maxPoolSize,
                                                        poolName,
                                                        null);
    }

    public static HConnectionPool newConnectionPool(final int initialPoolSize,
                                                    final int maxPoolSize,
                                                    final String poolName,
                                                    final HBaseConfiguration config) throws HBqlException {

        if (Utils.isValidString(poolName) && getConnectionPoolMap().containsKey(poolName))
            throw new HBqlException("Connection pool already exists: " + poolName);

        final HConnectionPoolImpl connectionPool = new HConnectionPoolImpl(initialPoolSize,
                                                                           maxPoolSize,
                                                                           poolName,
                                                                           config,
                                                                           getMaxPoolReferencesPerTablePerConnection());
        // Add to map if it has valid name
        if (Utils.isValidString(connectionPool.getName()))
            getConnectionPoolMap().put(connectionPool.getName(), connectionPool);

        return connectionPool;
    }


    public static int getMaxPoolReferencesPerTablePerConnection() {
        return maxPoolReferencesPerTablePerConnection;
    }

    public static void setMaxPoolReferencesPerTablePerConnection(final int maxPoolReferencesPerTablePerConnection) {
        HConnectionPoolManager.maxPoolReferencesPerTablePerConnection = maxPoolReferencesPerTablePerConnection;
    }

    public static HConnectionPool getConnectionPool(final String name) {
        return HConnectionPoolManager.getConnectionPoolMap().get(name);
    }

    private static Map<String, HConnectionPool> getConnectionPoolMap() {
        return HConnectionPoolManager.connectionPoolMap;
    }
}