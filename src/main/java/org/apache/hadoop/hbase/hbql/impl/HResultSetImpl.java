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

import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.literal.DateLiteral;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.mapping.MappingContext;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;
import org.apache.hadoop.hbase.hbql.util.Lists;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class HResultSetImpl<T, R> implements HResultSet<T> {

    // Record count keeps track of values that have evaluated as true and returned to user
    private final AtomicLong returnedRecordCount = new AtomicLong(0L);
    private final long returnedRecordLimit;

    private AtomicBoolean atomicClosed = new AtomicBoolean(false);
    private final List<ResultScanner> resultScannerList = Lists.newArrayList();
    private ResultScanner currentResultScanner = null;
    private AggregateRecord aggregateRecord;
    private final HTableWrapper tableWrapper;
    private final AtomicBoolean tableWrapperClosed = new AtomicBoolean(false);

    private final Query<T> query;
    private final ExpressionTree clientExpressionTree;
    private final CompletionQueueExecutor<R> completionQueueExecutor;

    protected HResultSetImpl(final Query<T> query,
                             final CompletionQueueExecutor<R> completionQueueExecutor) throws HBqlException {
        this.query = query;
        this.completionQueueExecutor = completionQueueExecutor;
        this.clientExpressionTree = this.getWithArgs().getClientExpressionTree();
        this.returnedRecordLimit = this.getWithArgs().getLimit();

        //this.setTableWrapper(this.getHConnectionImpl().newHTableWrapper(this.getWithArgs(), this.getTableName()));
        this.tableWrapper = this.getHConnectionImpl().newHTableWrapper(this.getWithArgs(), this.getTableName());
        this.getQuery().getSelectStmt().determineIfAggregateQuery();

        if (this.getSelectStmt().isAnAggregateQuery())
            this.setAggregateRecord(AggregateRecord.newAggregateRecord(this.getQuery().getSelectStmt()));

        // Set it once per evaluation
        DateLiteral.resetNow();

        if (this.getCompletionQueueExecutor() != null) {
            // Submit work to executor
            final List<RowRequest> rowRequestList = this.getQuery().getRowRequestList();
            this.getCompletionQueueExecutor().submitWorkToSubmitterThread(
                    new Runnable() {
                        public void run() {
                            submitWork(rowRequestList);
                            getCompletionQueueExecutor().putCompletion();
                        }
                    }
            );
        }
    }

    protected abstract void submitWork(final List<RowRequest> rowRequestList);

    public abstract Iterator<T> iterator();

    protected void cleanUpAtEndOfIterator(final boolean fromExceptionCatch) {

        try {
            if (!fromExceptionCatch)
                this.getQuery().callOnQueryComplete();

            try {
                if (this.getTableWrapper() != null)
                    this.getTableWrapper().getHTable().close();
            }
            catch (IOException e) {
                // No op
                e.printStackTrace();
            }
        }
        finally {
            // release to table pool
            //if (this.getTableWrapper() != null)
            if (!this.tableWrapperClosed.get())
                this.getTableWrapper().releaseHTable();

            this.tableWrapperClosed.set(true);
            //this.setTableWrapper(null);

            this.close();
        }
    }

    protected CompletionQueueExecutor<R> getCompletionQueueExecutor() {
        return this.completionQueueExecutor;
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

    private AtomicBoolean getAtomicClosed() {
        return this.atomicClosed;
    }

    public boolean isClosed() {
        return this.getAtomicClosed().get();
    }

    public synchronized void close() {
        if (!this.isClosed()) {
            for (final ResultScanner scanner : this.getResultScannerList())
                closeResultScanner(scanner, false);

            this.getResultScannerList().clear();

            if (this.getCompletionQueueExecutor() != null) {
                this.getCompletionQueueExecutor().close();
            }

            this.getAtomicClosed().set(true);
        }
    }

    protected long getReturnedRecordCount() {
        return this.returnedRecordCount.get();
    }

    protected void incrementRecordCount() {
        this.returnedRecordCount.incrementAndGet();
    }

    protected boolean returnedRecordLimitMet() {
        return this.getReturnedRecordLimit() > 0 && this.getReturnedRecordCount() > this.getReturnedRecordLimit();
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

    protected HTableWrapper getTableWrapper() {
        return this.tableWrapper;
    }

    protected HConnectionImpl getHConnectionImpl() {
        return this.getQuery().getHConnectionImpl();
    }

    protected SelectStatement getSelectStmt() {
        return this.getQuery().getSelectStmt();
    }

    protected MappingContext getMappingContext() {
        return this.getSelectStmt().getMappingContext();
    }

    protected String getTableName() {
        return this.getMappingContext().getMapping().getTableName();
    }

    protected WithArgs getWithArgs() {
        return this.getSelectStmt().getWithArgs();
    }
}
