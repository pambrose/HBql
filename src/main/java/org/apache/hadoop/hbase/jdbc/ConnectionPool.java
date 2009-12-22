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

package org.apache.hadoop.hbase.jdbc;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnectionPool;
import org.apache.hadoop.hbase.hbql.client.HConnectionPoolManager;
import org.apache.hadoop.hbase.jdbc.impl.ConnectionImpl;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import java.io.PrintWriter;
import java.sql.SQLException;

public class ConnectionPool implements ConnectionPoolDataSource {

    private final HConnectionPool connectionPool;

    public ConnectionPool(final int initPoolSize,
                          final int maxConnectionPoolSize) throws HBqlException {
        this(initPoolSize, maxConnectionPoolSize, null, null);
    }

    public ConnectionPool(final int initPoolSize,
                          final int maxConnectionPoolSize,
                          final String connectionPoolName) throws HBqlException {
        this(initPoolSize, maxConnectionPoolSize, connectionPoolName, null);
    }

    public ConnectionPool(final int initConnectionPoolSize,
                          final int maxConnectionPoolSize,
                          final HBaseConfiguration config) throws HBqlException {
        this(initConnectionPoolSize, maxConnectionPoolSize, null, config);
    }

    public ConnectionPool(final int initPoolSize,
                          final int maxConnectionPoolSize,
                          final String poolName,
                          final HBaseConfiguration config) throws HBqlException {
        this.connectionPool = HConnectionPoolManager.newConnectionPool(initPoolSize,
                                                                       maxConnectionPoolSize,
                                                                       poolName,
                                                                       config);
    }

    public static int getMaxPoolReferencesPerTablePerConnection() {
        return HConnectionPoolManager.getMaxPoolReferencesPerTablePerConnection();
    }

    public static void setMaxPoolReferencesPerTablePerConnection(final int maxPoolReferencesPerTablePerConnection) {
        HConnectionPoolManager.setMaxPoolReferencesPerTablePerConnection(maxPoolReferencesPerTablePerConnection);
    }

    private HConnectionPool getConnectionPool() {
        return this.connectionPool;
    }

    public PooledConnection getPooledConnection() throws SQLException {
        return new ConnectionImpl(this.getConnectionPool().takeConnection());
    }

    public PooledConnection getPooledConnection(final String s, final String s1) throws SQLException {
        return this.getPooledConnection();
    }

    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    public void setLogWriter(final PrintWriter printWriter) throws SQLException {

    }

    public void setLoginTimeout(final int i) throws SQLException {

    }

    public int getLoginTimeout() throws SQLException {
        return 0;
    }
}
