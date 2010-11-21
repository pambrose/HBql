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

package org.apache.hadoop.hbase.jdbc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.util.AtomicReferences;
import org.apache.hadoop.hbase.hbql.util.Lists;

import javax.sql.*;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectionImpl implements Connection, PooledConnection {

    private final HConnectionImpl hconnectionImpl;

    private AtomicReference<List<ConnectionEventListener>> connectionEventListenerList = AtomicReferences
            .newAtomicReference();
    private AtomicReference<List<StatementEventListener>>  statementEventListenerList  = AtomicReferences
            .newAtomicReference();

    public ConnectionImpl(final Configuration config, final int maxPoolReferencesPerTable) throws HBqlException {
        this.hconnectionImpl = new HConnectionImpl(config, null, maxPoolReferencesPerTable);
    }

    public ConnectionImpl(final HConnection hconnectionImpl) {
        this.hconnectionImpl = (HConnectionImpl) hconnectionImpl;
    }

    public HConnectionImpl getHConnectionImpl() {
        return this.hconnectionImpl;
    }

    public Connection getConnection() throws SQLException {
        return this;
    }

    public HConnection getHConnection() {
        return this.getHConnectionImpl();
    }

    public Statement createStatement() throws SQLException {
        return new StatementImpl(this);
    }

    public PreparedStatement prepareStatement(final String sql) throws SQLException {
        return new PreparedStatementImpl(this, sql);
    }

    public CallableStatement prepareCall(final String s) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String nativeSQL(final String s) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setAutoCommit(final boolean b) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void close() throws SQLException {
        try {
            this.getHConnectionImpl().close();
        }
        finally {
            this.fireConnectionClosed();
        }
    }

    public boolean isClosed() throws SQLException {
        return this.getHConnectionImpl().isClosed();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    public void setReadOnly(final boolean b) throws SQLException {

    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public void setCatalog(final String s) throws SQLException {

    }

    public String getCatalog() throws SQLException {
        return null;
    }

    public void setTransactionIsolation(final int i) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getTransactionIsolation() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {

    }

    public Statement createStatement(final int i, final int i1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public PreparedStatement prepareStatement(final String s, final int i, final int i1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public CallableStatement prepareCall(final String s, final int i, final int i1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    public void setTypeMap(final Map<String, Class<?>> stringClassMap) throws SQLException {

    }

    public void setHoldability(final int i) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Savepoint setSavepoint(final String s) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void rollback(final Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Statement createStatement(final int i, final int i1, final int i2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public PreparedStatement prepareStatement(final String s, final int i, final int i1, final int i2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public CallableStatement prepareCall(final String s, final int i, final int i1, final int i2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public PreparedStatement prepareStatement(final String s, final int i) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public PreparedStatement prepareStatement(final String s, final int[] ints) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public PreparedStatement prepareStatement(final String s, final String[] strings) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isValid(final int i) throws SQLException {
        return false;
    }

    public void setClientInfo(final String s, final String s1) throws SQLClientInfoException {

    }

    public void setClientInfo(final Properties properties) throws SQLClientInfoException {

    }

    public String getClientInfo(final String s) throws SQLException {
        return null;
    }

    public Properties getClientInfo() throws SQLException {
        return null;
    }

    public Array createArrayOf(final String s, final Object[] objects) throws SQLException {
        return null;
    }

    public Struct createStruct(final String s, final Object[] objects) throws SQLException {
        return null;
    }

    public <T> T unwrap(final Class<T> tClass) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(final Class<?> aClass) throws SQLException {
        return false;
    }

    private AtomicReference<List<ConnectionEventListener>> getAtomicConnectionEventListenerList() {
        return this.connectionEventListenerList;
    }

    private List<ConnectionEventListener> getConnectionEventListenerList() {
        if (this.getAtomicConnectionEventListenerList().get() == null)
            synchronized (this) {
                if (this.getAtomicConnectionEventListenerList().get() == null) {
                    final List<ConnectionEventListener> val = Lists.newArrayList();
                    this.getAtomicConnectionEventListenerList().set(val);
                }
            }
        return this.getAtomicConnectionEventListenerList().get();
    }

    private void fireConnectionClosed() {
        if (this.getAtomicConnectionEventListenerList().get() != null) {
            for (final ConnectionEventListener listener : this.getConnectionEventListenerList())
                listener.connectionClosed(new ConnectionEvent(this));
        }
    }

    private AtomicReference<List<StatementEventListener>> getAtomicStatementEventListenerList() {
        return this.statementEventListenerList;
    }

    private List<StatementEventListener> getStatementEventListenerList() {
        if (this.getAtomicStatementEventListenerList().get() == null)
            synchronized (this) {
                if (this.getAtomicStatementEventListenerList().get() == null) {
                    final List<StatementEventListener> val = Lists.newArrayList();
                    this.getAtomicStatementEventListenerList().set(val);
                }
            }
        return this.getAtomicStatementEventListenerList().get();
    }

    void fireStatementClosed(final PreparedStatement pstmt) {
        if (this.getAtomicStatementEventListenerList().get() != null) {
            for (final StatementEventListener listener : this.getStatementEventListenerList())
                listener.statementClosed(new StatementEvent(this, pstmt));
        }
    }

    public void addConnectionEventListener(final ConnectionEventListener connectionEventListener) {
        this.getConnectionEventListenerList().add(connectionEventListener);
    }

    public void removeConnectionEventListener(final ConnectionEventListener connectionEventListener) {
        this.getConnectionEventListenerList().remove(connectionEventListener);
    }

    public void addStatementEventListener(final StatementEventListener statementEventListener) {
        this.getStatementEventListenerList().add(statementEventListener);
    }

    public void removeStatementEventListener(final StatementEventListener statementEventListener) {
        this.getStatementEventListenerList().remove(statementEventListener);
    }
}
