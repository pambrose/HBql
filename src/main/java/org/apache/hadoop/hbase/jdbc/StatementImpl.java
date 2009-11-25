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

import org.apache.expreval.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.Query;
import org.apache.hadoop.hbase.hbql.impl.Util;
import org.apache.hadoop.hbase.hbql.statement.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.HBqlStatement;
import org.apache.hadoop.hbase.hbql.statement.NonConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

public class StatementImpl implements Statement {

    private final ConnectionImpl jdbcConnection;
    private final HConnectionImpl hConnection;

    private ResultSet resultSet = null;

    public StatementImpl(final ConnectionImpl jdbcConnection, final HConnectionImpl hConnection) {
        this.jdbcConnection = jdbcConnection;
        this.hConnection = hConnection;
    }

    protected ConnectionImpl getJdbcConnection() {
        return this.jdbcConnection;
    }

    protected HConnectionImpl getHConnectionImpl() {
        return this.hConnection;
    }

    public HConnection getHConnection() {
        return this.getHConnectionImpl();
    }

    public Connection getConnection() {
        return this.getJdbcConnection();
    }

    public int executeUpdate(final HBqlStatement statement) throws HBqlException {

        if (Util.isSelectStatement(statement)) {
            throw new HBqlException("executeUpdate() requires a non-SELECT statement");
        }
        else if (Util.isDMLStatement(statement)) {
            final ConnectionStatement stmt = ((ConnectionStatement)statement);
            final ExecutionResults results = stmt.evaluatePredicateAndExecute(this.getHConnectionImpl());
            return results.getCount();
        }
        else if (Util.isConnectionStatemet(statement)) {
            final ConnectionStatement stmt = ((ConnectionStatement)statement);
            stmt.evaluatePredicateAndExecute(this.getHConnectionImpl());
            return 0;
        }
        else if (Util.isNonConectionStatemet(statement)) {
            final NonConnectionStatement stmt = ((NonConnectionStatement)statement);
            stmt.execute();
            return 0;
        }
        else {
            throw new InternalErrorException("Bad state with " + statement.getClass().getSimpleName());
        }
    }

    protected ResultSet executeQuery(final HBqlStatement statement) throws HBqlException {

        if (!Util.isSelectStatement(statement))
            throw new HBqlException("executeQuery() requires a SELECT statement");

        final Query<HRecord> query = Query.newQuery(this.getHConnectionImpl(),
                                                    (SelectStatement)statement,
                                                    HRecord.class);
        this.setResultSet(new ResultSetImpl(this, query.getResults()));
        return this.getResultSet();
    }

    protected boolean execute(final HBqlStatement statement) throws HBqlException {
        if (Util.isSelectStatement(statement)) {
            this.executeQuery(statement);
            return true;
        }
        else {
            executeUpdate(statement);
            return false;
        }
    }

    public boolean execute(final String sql) throws HBqlException {
        return execute(Util.parseJdbcStatement(sql));
    }

    public ResultSet executeQuery(final String sql) throws HBqlException {
        return this.executeQuery(Util.parseJdbcStatement(sql));
    }

    public int executeUpdate(final String sql) throws SQLException {
        return this.executeUpdate(Util.parseJdbcStatement(sql));
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

    protected void setResultSet(final ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public ResultSet getResultSet() {
        return this.resultSet;
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
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isClosed() throws SQLException {
        return false;
    }

    public void setPoolable(final boolean b) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isPoolable() throws SQLException {
        return false;
    }

    public <T> T unwrap(final Class<T> tClass) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(final Class<?> aClass) throws SQLException {
        return false;
    }
}
