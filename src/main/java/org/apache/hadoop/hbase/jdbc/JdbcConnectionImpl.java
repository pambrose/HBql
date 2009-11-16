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
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

public class JdbcConnectionImpl implements Connection {

    private final HConnectionImpl hbqlConnection;

    public JdbcConnectionImpl(final String name, final HBaseConfiguration config) {
        this.hbqlConnection = new HConnectionImpl(name, config);
    }

    private HConnectionImpl getHbqlConnection() {
        return this.hbqlConnection;
    }

    public Statement createStatement() throws SQLException {
        return new JdbcStatementImpl(this, this.getHbqlConnection());
    }

    public PreparedStatement prepareStatement(final String sql) throws SQLException {
        return new JdbcPreparedStatementImpl(this, this.getHbqlConnection(), sql);
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
        throw new SQLFeatureNotSupportedException();
    }

    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void close() throws SQLException {
        this.getHbqlConnection().close();
    }

    public boolean isClosed() throws SQLException {
        return this.getHbqlConnection().isClosed();
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
        return null;
    }

    public PreparedStatement prepareStatement(final String s, final int i, final int i1, final int i2) throws SQLException {
        return null;
    }

    public CallableStatement prepareCall(final String s, final int i, final int i1, final int i2) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(final String s, final int i) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(final String s, final int[] ints) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(final String s, final String[] strings) throws SQLException {
        return null;
    }

    public Clob createClob() throws SQLException {
        return null;
    }

    public Blob createBlob() throws SQLException {
        return null;
    }

    public NClob createNClob() throws SQLException {
        return null;
    }

    public SQLXML createSQLXML() throws SQLException {
        return null;
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
}
