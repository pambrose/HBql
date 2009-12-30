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

import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.literal.DateLiteral;
import org.apache.expreval.util.ExecutorWithQueue;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.QueryListener;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public abstract class HResultSetImpl<T, R> implements HResultSet<T> {

    // Record count keeps track of values that have evaluated as true and returned to user
    private final AtomicLong returnedRecordCount = new AtomicLong(0L);
    private final long returnedRecordLimit;

    private final List<ResultScanner> resultScannerList = Lists.newArrayList();
    private ResultScanner currentResultScanner = null;
    private AggregateRecord aggregateRecord;

    private final Query<T> query;
    private final ExpressionTree clientExpressionTree;
    private HTableWrapper tableWrapper;
    private final ExecutorWithQueue<R> executor;

    private volatile boolean closed = false;

    protected HResultSetImpl(final Query<T> query, final ExecutorWithQueue<R> executor) throws HBqlException {
        this.query = query;
        this.executor = executor;

        this.clientExpressionTree = this.getWithArgs().getClientExpressionTree();
        this.returnedRecordLimit = this.getWithArgs().getLimit();
        this.tableWrapper = this.getHConnectionImpl().newHTableWrapper(this.getWithArgs(), this.getTableName());

        this.getQuery().getSelectStmt().determineIfAggregateQuery();

        this.setAggregateRecord(AggregateRecord.newAggregateRecord(this.getQuery().getSelectStmt()));

        // Set it once per evaluation
        DateLiteral.resetNow();

        if (this.getListeners() != null) {
            for (final QueryListener<T> listener : this.getListeners())
                listener.onQueryInit();
        }

        // Submit work to executor
        this.submitWork();
    }

    protected abstract void submitWork() throws HBqlException;

    public abstract Iterator<T> iterator();

    protected ExecutorWithQueue<R> getExecutor() {
        return this.executor;
    }

    protected void setAggregateRecord(final AggregateRecord aggregateRecord) {
        this.aggregateRecord = aggregateRecord;
    }

    protected AggregateRecord getAggregateRecord() {
        return this.aggregateRecord;
    }

    protected List<ResultScanner> getResultScannerList() {
        return this.resultScannerList;
    }

    protected ResultScanner getCurrentResultScanner() {
        return this.currentResultScanner;
    }

    public boolean isClosed() {
        return this.closed;
    }

    protected void setCurrentResultScanner(final ResultScanner currentResultScanner) {
        // First close previous ResultScanner before reassigning
        closeResultScanner(getCurrentResultScanner(), true);
        this.currentResultScanner = currentResultScanner;
        getResultScannerList().add(getCurrentResultScanner());
    }

    protected void closeResultScanner(final ResultScanner scanner, final boolean removeFromList) {
        if (scanner != null) {
            try {
                scanner.close();
            }
            catch (Exception e) {
                // Do nothing
            }
            if (removeFromList)
                getResultScannerList().remove(scanner);
        }
    }

    public void close() {
        if (!this.isClosed()) {
            synchronized (this) {
                if (!this.isClosed()) {
                    for (final ResultScanner scanner : this.getResultScannerList())
                        closeResultScanner(scanner, false);
                    this.getResultScannerList().clear();
                    if (this.getExecutor() != null)
                        this.getExecutor().release();
                    this.closed = true;
                }
            }
        }
    }

    protected long getReturnedRecordCount() {
        return this.returnedRecordCount.get();
    }

    protected void incrementRecordCount() {
        this.returnedRecordCount.incrementAndGet();
    }

    protected boolean returnedRecordLimitMet() {
        return this.getReturnedRecordLimit() > 0 && this.getReturnedRecordCount() >= this.getReturnedRecordLimit();
    }

    protected long getReturnedRecordLimit() {
        return this.returnedRecordLimit;
    }

    protected int getMaxVersions() throws HBqlException {
        return this.getQuery().getSelectStmt().getWithArgs().getMaxVersions();
    }

    protected Query<T> getQuery() {
        return this.query;
    }

    protected ExpressionTree getClientExpressionTree() {
        return this.clientExpressionTree;
    }

    protected HTableWrapper getHTableWrapper() {
        return this.tableWrapper;
    }

    protected void setTableWrapper(final HTableWrapper tableWrapper) {
        this.tableWrapper = tableWrapper;
    }

    public void addQueryListener(QueryListener<T> listener) {
        this.getQuery().addListener(listener);
    }

    public void clearQueryListeners() {
        this.getQuery().clearListeners();
    }

    protected HConnectionImpl getHConnectionImpl() {
        return this.getQuery().getHConnectionImpl();
    }

    protected SelectStatement getSelectStmt() {
        return this.getQuery().getSelectStmt();
    }

    protected String getTableName() {
        return this.getSelectStmt().getMapping().getTableName();
    }

    protected WithArgs getWithArgs() {
        return this.getSelectStmt().getWithArgs();
    }

    protected List<QueryListener<T>> getListeners() {
        return this.getQuery().getListeners();
    }
}
