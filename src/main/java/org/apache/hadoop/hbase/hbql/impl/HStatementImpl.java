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

package org.apache.hadoop.hbase.hbql.impl;

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.HStatement;
import org.apache.hadoop.hbase.hbql.statement.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.HBqlStatement;
import org.apache.hadoop.hbase.hbql.statement.NonConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import java.util.List;


public class HStatementImpl implements HStatement {

    private final HConnectionImpl connectionImpl;
    private HResultSetImpl resultSet = null;
    private volatile boolean closed = false;

    public HStatementImpl(final HConnectionImpl connectionImpl) {
        this.connectionImpl = connectionImpl;
    }

    protected HConnectionImpl getHConnectionImpl() {
        return this.connectionImpl;
    }

    public <T> HResultSet<T> getResultSet() {
        return (HResultSet<T>)this.resultSet;
    }

    public ExecutionResults executeUpdate(final HBqlStatement statement) throws HBqlException {

        if (Util.isSelectStatement(statement)) {
            throw new HBqlException("executeUpdate() requires a non-SELECT statement");
        }
        else if (Util.isConnectionStatemet(statement)) {
            return ((ConnectionStatement)statement).evaluatePredicateAndExecute(this.getHConnectionImpl());
        }
        else if (Util.isNonConectionStatemet(statement)) {
            return ((NonConnectionStatement)statement).execute();
        }
        else {
            throw new InternalErrorException("Bad state with " + statement.getClass().getSimpleName());
        }
    }

    protected <T> HResultSet<T> executeQuery(final HBqlStatement statement, final Class clazz) throws HBqlException {

        if (!Util.isSelectStatement(statement))
            throw new HBqlException("executeQuery() requires a SELECT statement");

        final Query<T> query = Query.newQuery(this.getHConnectionImpl(), (SelectStatement)statement, clazz);
        final HResultSetImpl<T> resultSetImpl = new HResultSetImpl<T>(query);

        this.resultSet = resultSetImpl;
        return resultSetImpl;
    }

    protected <T> List<T> executeQueryAndFetch(final HBqlStatement statement, final Class clazz) throws HBqlException {

        final List<T> retval = Lists.newArrayList();

        HResultSet<T> results = null;

        try {
            results = this.executeQuery(statement, clazz);

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
        if (Util.isSelectStatement(statement)) {
            this.executeQuery(statement, null);
            return new ExecutionResults("Query executed");
        }
        else {
            return this.executeUpdate(statement);
        }
    }

    public ExecutionResults execute(final String sql) throws HBqlException {
        return this.execute(Util.parseJdbcStatement(sql));
    }

    public HResultSet<HRecord> executeQuery(final String sql) throws HBqlException {
        return this.executeQuery(Util.parseJdbcStatement(sql), HRecord.class);
    }

    public <T> HResultSet<T> executeQuery(final String sql, final Class clazz) throws HBqlException {
        return this.executeQuery(Util.parseJdbcStatement(sql), clazz);
    }

    public List<HRecord> executeQueryAndFetch(final String sql) throws HBqlException {
        return this.executeQueryAndFetch(Util.parseJdbcStatement(sql), HRecord.class);
    }

    public <T> List<T> executeQueryAndFetch(final String sql, final Class clazz) throws HBqlException {
        return this.executeQueryAndFetch(Util.parseJdbcStatement(sql), clazz);
    }

    public ExecutionResults executeUpdate(final String sql) throws HBqlException {
        return this.executeUpdate(Util.parseJdbcStatement(sql));
    }

    public synchronized void close() throws HBqlException {

        if (!isClosed()) {
            this.closed = true;

            if (this.getResultSet() != null)
                this.getResultSet().close();
        }

        if (this.getHConnectionImpl().getRawConnectionEventListenerList() != null) {
            for (final ConnectionEventListener listener : this.getHConnectionImpl().getConnectionEventListenerList())
                listener.connectionClosed(new ConnectionEvent(this.getHConnectionImpl()));
        }
    }

    public boolean isClosed() {
        return this.closed;
    }
}
