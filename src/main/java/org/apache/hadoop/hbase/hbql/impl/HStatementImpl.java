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

package org.apache.hadoop.hbase.hbql.impl;

import org.apache.expreval.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.HStatement;
import org.apache.hadoop.hbase.hbql.client.QueryFuture;
import org.apache.hadoop.hbase.hbql.client.QueryListener;
import org.apache.hadoop.hbase.hbql.statement.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.HBqlStatement;
import org.apache.hadoop.hbase.hbql.statement.NonConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.util.Lists;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


public class HStatementImpl implements HStatement {

    private final AtomicBoolean atomicClosed = new AtomicBoolean(false);
    private final HConnectionImpl connection;
    private HResultSet resultSet = null;
    private boolean ignoreQueryExecutor = false;

    public HStatementImpl(final HConnectionImpl conn) {
        this.connection = conn;
    }

    protected HConnectionImpl getHConnectionImpl() {
        return this.connection;
    }

    public <T> HResultSet<T> getResultSet() {
        return (HResultSet<T>)this.resultSet;
    }

    public boolean getIgnoreQueryExecutor() {
        return this.ignoreQueryExecutor;
    }

    public void setIgnoreQueryExecutor(final boolean ignoreQueryExecutor) {
        this.ignoreQueryExecutor = ignoreQueryExecutor;
    }

    public ExecutionResults executeUpdate(final HBqlStatement statement) throws HBqlException {

        if (Utils.isSelectStatement(statement)) {
            throw new HBqlException("executeUpdate() requires a non-SELECT statement");
        }
        else if (Utils.isConnectionStatemet(statement)) {
            return ((ConnectionStatement)statement).evaluatePredicateAndExecute(this.getHConnectionImpl());
        }
        else if (Utils.isNonConectionStatemet(statement)) {
            return ((NonConnectionStatement)statement).execute();
        }
        else {
            throw new InternalErrorException("Bad state with " + statement.getClass().getSimpleName());
        }
    }

    protected <T> HResultSet<T> executeQuery(final HBqlStatement statement,
                                             final Class clazz,
                                             final QueryListener<T>... listeners) throws HBqlException {

        if (!Utils.isSelectStatement(statement))
            throw new HBqlException("executeQuery() requires a SELECT statement");

        final Query<T> query = Query.newQuery(this.getHConnectionImpl(), (SelectStatement)statement, clazz, listeners);

        this.resultSet = query.newResultSet(this.getIgnoreQueryExecutor());

        return this.resultSet;
    }

    protected <T> QueryFuture executeQueryAsync(final HBqlStatement statement,
                                                final Class clazz,
                                                final QueryListener<T>... listeners) throws HBqlException {

        if (!Utils.isSelectStatement(statement))
            throw new HBqlException("executeQueryAsync() requires a SELECT statement");

        final Query<T> query = Query.newQuery(this.getHConnectionImpl(), (SelectStatement)statement, clazz, listeners);

        final UnboundedAsyncExecutor asyncExecutor = this.getHConnectionImpl().getAsyncExecutorForConnection();

        return asyncExecutor.submit(
                new AsyncRunnable() {
                    public void run() {
                        try {
                            final HResultSet<T> rs = query.newResultSet(false);
                            for (T rec : rs) {
                                // Iterate through the results
                            }
                            rs.close();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                            this.getQueryFuture().setCaughtException(e);
                        }
                        finally {
                            asyncExecutor.close();
                        }
                    }
                });
    }

    protected <T> List<T> executeQueryAndFetch(final HBqlStatement statement,
                                               final Class clazz,
                                               final QueryListener<T>... listeners) throws HBqlException {

        final List<T> retval = Lists.newArrayList();

        HResultSet<T> results = null;

        try {
            results = this.executeQuery(statement, clazz, listeners);

            for (final T val : results)
                retval.add(val);
        }
        finally {
            if (results != null)
                results.close();
        }

        return retval;
    }

    protected ExecutionResults execute(final HBqlStatement statement) throws HBqlException {
        if (Utils.isSelectStatement(statement)) {
            this.executeQuery(statement, null);
            return new ExecutionResults("Query executed");
        }
        else {
            return this.executeUpdate(statement);
        }
    }

    public ExecutionResults execute(final String sql) throws HBqlException {
        return this.execute(Utils.parseHBqlStatement(sql));
    }

    public HResultSet<HRecord> executeQuery(final String sql,
                                            final QueryListener<HRecord>... listeners) throws HBqlException {
        return this.executeQuery(Utils.parseHBqlStatement(sql), HRecord.class, listeners);
    }

    public QueryFuture executeQueryAsync(final String sql,
                                         final QueryListener<HRecord>... listeners) throws HBqlException {
        return this.executeQueryAsync(Utils.parseHBqlStatement(sql), HRecord.class, listeners);
    }

    public <T> HResultSet<T> executeQuery(final String sql,
                                          final Class clazz,
                                          final QueryListener<T>... listeners) throws HBqlException {
        return this.executeQuery(Utils.parseHBqlStatement(sql), clazz, listeners);
    }

    public <T> QueryFuture executeQueryAsync(final String sql,
                                             final Class clazz,
                                             final QueryListener<T>... listeners) throws HBqlException {
        return this.executeQueryAsync(Utils.parseHBqlStatement(sql), clazz, listeners);
    }

    public List<HRecord> executeQueryAndFetch(final String sql,
                                              final QueryListener<HRecord>... listeners) throws HBqlException {
        return this.executeQueryAndFetch(Utils.parseHBqlStatement(sql), HRecord.class, listeners);
    }

    public <T> List<T> executeQueryAndFetch(final String sql,
                                            final Class clazz,
                                            final QueryListener<T>... listeners) throws HBqlException {
        return this.executeQueryAndFetch(Utils.parseHBqlStatement(sql), clazz, listeners);
    }

    public ExecutionResults executeUpdate(final String sql) throws HBqlException {
        return this.executeUpdate(Utils.parseHBqlStatement(sql));
    }

    private AtomicBoolean getAtomicClosed() {
        return this.atomicClosed;
    }

    public boolean isClosed() {
        return this.getAtomicClosed().get();
    }

    public synchronized void close() throws HBqlException {
        if (!this.isClosed()) {
            if (this.getResultSet() != null)
                this.getResultSet().close();

            this.getAtomicClosed().set(true);
        }
    }
}
