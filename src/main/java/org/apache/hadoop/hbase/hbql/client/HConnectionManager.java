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

import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

import java.util.Map;

public class HConnectionManager {

    private static Map<String, HConnection> connectionMap = Maps.newHashMap();

    public static HConnection newConnection() throws HBqlException {
        return newConnection(null, null);
    }

    public static HConnection newConnection(final HBaseConfiguration config) throws HBqlException {
        return newConnection(null, config);
    }

    public static synchronized HConnection newConnection(final String name) throws HBqlException {
        return newConnection(name, null);
    }

    public static synchronized HConnection newConnection(final String name,
                                                         final HBaseConfiguration config) throws HBqlException {
        final HConnectionImpl connection = new HConnectionImpl(name, config);

        if (connection.getName() != null)
            HConnectionManager.getConnectionMap().put(connection.getName(), connection);

        return connection;
    }

    public static HConnection getConnection(final String name) {
        return HConnectionManager.getConnectionMap().get(name);
    }

    private static Map<String, HConnection> getConnectionMap() {
        return HConnectionManager.connectionMap;
    }
}