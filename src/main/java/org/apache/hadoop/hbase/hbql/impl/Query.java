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

import org.apache.expreval.util.ExecutorWithQueue;
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Sets;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.QueryListener;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.HRecordResultAccessor;
import org.apache.hadoop.hbase.hbql.mapping.ResultAccessor;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;

import java.util.List;
import java.util.Set;

public class Query<T> {

    private final HConnectionImpl connection;
    private final SelectStatement selectStatement;
    private List<QueryListener<T>> listeners = null;

    private Query(final HConnectionImpl conn, final SelectStatement selectStatement) throws HBqlException {
        this.connection = conn;
        this.selectStatement = selectStatement;

        this.getSelectStmt().validate(this.getHConnectionImpl());
        this.getSelectStmt().validateTypes();
    }

    public static <E> Query<E> newQuery(final HConnectionImpl conn,
                                        final SelectStatement selectStatement,
                                        final Class clazz) throws HBqlException {
        final ResultAccessor accessor;
        if (clazz.equals(HRecord.class)) {
            accessor = new HRecordResultAccessor(selectStatement);
        }
        else {
            accessor = conn.getAnnotationMapping(clazz);
            if (accessor == null)
                throw new HBqlException("Unknown class " + clazz.getName());
        }

        selectStatement.setResultAccessor(accessor);

        return new Query<E>(conn, selectStatement);
    }

    public synchronized void addListener(final QueryListener<T> listener) {
        if (this.getListeners() == null)
            this.listeners = Lists.newArrayList();

        this.getListeners().add(listener);
    }

    public HConnectionImpl getHConnectionImpl() {
        return this.connection;
    }

    public SelectStatement getSelectStmt() {
        return this.selectStatement;
    }

    public List<RowRequest> getRowRequestList() throws HBqlException {

        // Get list of all columns that are used in select list and expr tree
        final Set<ColumnAttrib> allAttribs = Sets.newHashSet();
        allAttribs.addAll(this.getSelectStmt().getSelectAttribList());

        final WithArgs withArgs = this.getSelectStmt().getWithArgs();
        allAttribs.addAll(withArgs.getColumnsUsedInAllWhereExprs());

        return withArgs.getRowRequestList(this.getHConnectionImpl(), this.getSelectStmt().getMapping(), allAttribs);
    }

    public List<QueryListener<T>> getListeners() {
        return this.listeners;
    }

    public void clearListeners() {
        if (this.getListeners() != null)
            this.getListeners().clear();
    }

    public HResultSet<T> newResultSet() throws HBqlException {
        if (this.getHConnectionImpl().usesAQueryExecutor()) {
            // This may block waiting for a Executor to become available from the ExecutorPool
            final ExecutorWithQueue executor = this.getHConnectionImpl().getExecutorForConnection();
            if (executor.threadsReadResults())
                return new ResultExecutorResultSet<T>(this, (ResultExecutor)executor);
            else
                return new ResultScannerExecutorResultSet<T>(this, (ResultScannerExecutor)executor);
        }
        else {
            return new NonExecutorResultSet<T>(this);
        }
    }
}
