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

package org.apache.hadoop.hbase.jdbc.impl;

import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;
import java.sql.Connection;
import java.sql.SQLException;

public class PooledConnectionImpl implements PooledConnection {

    private final ConnectionImpl connectionImpl;

    public PooledConnectionImpl(final ConnectionImpl connectionImpl) {
        this.connectionImpl = connectionImpl;
    }

    public Connection getConnection() throws SQLException {
        return this.getConnectionImpl();
    }

    private ConnectionImpl getConnectionImpl() {
        return this.connectionImpl;
    }

    public void close() throws SQLException {
        this.getConnectionImpl().close();
    }

    public void addConnectionEventListener(final ConnectionEventListener connectionEventListener) {

    }

    public void removeConnectionEventListener(final ConnectionEventListener connectionEventListener) {

    }

    public void addStatementEventListener(final StatementEventListener statementEventListener) {

    }

    public void removeStatementEventListener(final StatementEventListener statementEventListener) {

    }
}
