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

import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.Query;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

public class JdbcStatementImpl implements Statement {

    private final JdbcConnectionImpl connection;

    public JdbcStatementImpl(final JdbcConnectionImpl connection) {
        this.connection = connection;
    }

    private JdbcConnectionImpl getConnectionImpl() {
        return (JdbcConnectionImpl)this.getConnection();
    }

    public <T> T unwrap(final Class<T> tClass) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(final Class<?> aClass) throws SQLException {
        return false;
    }

    public boolean execute(final String sql) throws SQLException {
        return false;
    }

    public ResultSet executeQuery(final String sql) throws SQLException {
        final Query<HRecord> query = this.getConnectionImpl().newQuery(sql);
        return new JdbcResultSetImpl(this, query.getResults());
    }

    public int executeUpdate(final String sql) throws SQLException {
        return 0;
    }

    public void close() throws SQLException {

    }

    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    public void setMaxFieldSize(final int i) throws SQLException {

    }

    public int getMaxRows() throws SQLException {
        return 0;
    }

    public void setMaxRows(final int i) throws SQLException {

    }

    public void setEscapeProcessing(final boolean b) throws SQLException {

    }

    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    public void setQueryTimeout(final int i) throws SQLException {

    }

    public void cancel() throws SQLException {

    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {

    }

    public void setCursorName(final String s) throws SQLException {

    }

    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    public int getUpdateCount() throws SQLException {
        return 0;
    }

    public boolean getMoreResults() throws SQLException {
        return false;
    }

    public void setFetchDirection(final int i) throws SQLException {

    }

    public int getFetchDirection() throws SQLException {
        return 0;
    }

    public void setFetchSize(final int i) throws SQLException {

    }

    public int getFetchSize() throws SQLException {
        return 0;
    }

    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    public int getResultSetType() throws SQLException {
        return 0;
    }

    public void addBatch(final String s) throws SQLException {

    }

    public void clearBatch() throws SQLException {

    }

    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    public Connection getConnection() {
        return this.connection;
    }

    public boolean getMoreResults(final int i) throws SQLException {
        return false;
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    public int executeUpdate(final String s, final int i) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int executeUpdate(final String s, final int[] ints) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int executeUpdate(final String s, final String[] strings) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean execute(final String s, final int i) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean execute(final String s, final int[] ints) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean execute(final String s, final String[] strings) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    public boolean isClosed() throws SQLException {
        return false;
    }

    public void setPoolable(final boolean b) throws SQLException {

    }

    public boolean isPoolable() throws SQLException {
        return false;
    }
}
