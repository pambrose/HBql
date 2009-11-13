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

public class ConnectionManager {

    private static Map<String, HConnection> connectionMap = Maps.newHashMap();

    public static HConnection newConnection() {
        return newConnection(null, null);
    }

    public static HConnection newConnection(final HBaseConfiguration config) {
        return newConnection(null, config);
    }

    public static synchronized HConnection newConnection(final String name) {
        return newConnection(name, null);
    }

    public static synchronized HConnection newConnection(final String name, final HBaseConfiguration config) {
        final HConnectionImpl conn = new HConnectionImpl(name, config);

        if (conn.getName() != null)
            ConnectionManager.getConnectionMap().put(conn.getName(), conn);

        return conn;
    }

    public static HConnection getConnection(final String name) {
        return ConnectionManager.getConnectionMap().get(name);
    }

    private static Map<String, HConnection> getConnectionMap() {
        return ConnectionManager.connectionMap;
    }
}