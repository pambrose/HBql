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

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.literal.DateLiteral;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.QueryListener;
import org.apache.hadoop.hbase.hbql.schema.Mapping;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HResultSetImpl<T> implements HResultSet<T> {

    private final List<ResultScanner> resultScannerList = Lists.newArrayList();
    private final QueryImpl<T> query;

    HResultSetImpl(final QueryImpl<T> query) throws HBqlException {
        this.query = query;

        // Set it once per evaluation
        DateLiteral.resetNow();

        if (this.getListeners() != null) {
            for (final QueryListener<T> listener : this.getListeners())
                listener.onQueryInit();
        }

        this.getQuery().getSelectStatement().determineIfAggregateQuery();
    }

    private HConnection getConnection() {
        return this.getQuery().getHConnection();
    }

    private QueryImpl<T> getQuery() {
        return this.query;
    }

    public void addQueryListener(QueryListener<T> listener) {
        this.getQuery().addListener(listener);
    }

    public void clearQueryListeners() {
        this.getQuery().clearListeners();
    }

    private List<ResultScanner> getResultScannerList() {
        return this.resultScannerList;
    }

    private SelectStatement getSelectStmt() {
        return this.getQuery().getSelectStatement();
    }

    private WithArgs getWithArgs() {
        return this.getSelectStmt().getWithArgs();
    }

    private List<QueryListener<T>> getListeners() {
        return this.getQuery().getListeners();
    }

    private List<RowRequest> getRowRequestList() throws HBqlException {
        return this.getQuery().getRowRequestList();
    }

    public void close() {

        for (final ResultScanner scanner : this.getResultScannerList())
            closeResultScanner(scanner, false);

        this.getResultScannerList().clear();
    }

    private void closeResultScanner(final ResultScanner scanner, final boolean removeFromList) {

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

    public Iterator<T> iterator() {

        try {
            return new ResultsIterator<T>(this.getWithArgs().getLimit()) {

                private final HTable table = getConnection().getHTable(getSelectStmt().getSchema().getTableName());
                private final ExpressionTree clientExpressionTree = getWithArgs().getClientExpressionTree();
                private final Iterator<RowRequest> rowRequestIterator = getRowRequestList().iterator();

                private int maxVersions = 0;
                private ResultScanner currentResultScanner = null;
                private Iterator<Result> currentResultIterator = null;

                private AggregateRecord aggregateRecord = AggregateRecord.newAggregateRecord(getQuery().getSelectStatement(),
                                                                                             getSelectStmt());

                // Prime the iterator with the first value
                private T nextObject = fetchNextObject();

                private ExpressionTree getClientExpressionTree() {
                    return this.clientExpressionTree;
                }

                private Iterator<RowRequest> getRowRequestIterator() {
                    return this.rowRequestIterator;
                }

                private int getMaxVersions() {
                    return this.maxVersions;
                }

                private void setMaxVersions(final int maxVersions) {
                    this.maxVersions = maxVersions;
                }

                private ResultScanner getCurrentResultScanner() {
                    return this.currentResultScanner;
                }

                private Iterator<Result> getCurrentResultIterator() {
                    return this.currentResultIterator;
                }

                protected T getNextObject() {
                    return this.nextObject;
                }

                private HTable getTable() {
                    return this.table;
                }

                private void setCurrentResultScanner(final ResultScanner currentResultScanner) {
                    // First close previous ResultScanner before reassigning
                    closeResultScanner(this.getCurrentResultScanner(), true);
                    this.currentResultScanner = currentResultScanner;
                    getResultScannerList().add(this.getCurrentResultScanner());
                }

                private void setCurrentResultIterator(final Iterator<Result> currentResultIterator) {
                    this.currentResultIterator = currentResultIterator;
                }

                @SuppressWarnings("unchecked")
                protected T fetchNextObject() throws HBqlException {

                    final Mapping mapping = getQuery().getSelectStatement().getMapping();

                    while (this.getCurrentResultIterator() != null || this.getRowRequestIterator().hasNext()) {

                        if (this.getCurrentResultIterator() == null)
                            this.setCurrentResultIterator(getNextResultIterator());

                        while (this.getCurrentResultIterator().hasNext()) {

                            final Result result = this.getCurrentResultIterator().next();

                            try {
                                if (getClientExpressionTree() != null && !getClientExpressionTree().evaluate(result))
                                    continue;
                            }
                            catch (ResultMissingColumnException e) {
                                continue;
                            }

                            incrementReturnedRecordCount();

                            if (getSelectStmt().isAnAggregateQuery()) {
                                this.getAggregateRecord().applyValues(result);
                            }
                            else {
                                final T val = (T)mapping.newObject(getSelectStmt(),
                                                                   getSelectStmt().getSelectElementList(),
                                                                   this.getMaxVersions(),
                                                                   result);

                                if (getListeners() != null)
                                    for (final QueryListener<T> listener : getListeners())
                                        listener.onEachRow(val);

                                return val;
                            }
                        }

                        this.setCurrentResultIterator(null);

                        closeResultScanner(this.getCurrentResultScanner(), true);
                    }

                    if (getSelectStmt().isAnAggregateQuery() && this.getAggregateRecord() != null) {
                        // Stash the value and then null it out for next time through
                        final AggregateRecord retval = this.getAggregateRecord();
                        this.setAggregateRecord(null);
                        return (T)retval;
                    }

                    return null;
                }

                private Iterator<Result> getNextResultIterator() throws HBqlException {
                    final RowRequest rowRequest = this.getRowRequestIterator().next();
                    this.setMaxVersions(rowRequest.getMaxVersions());
                    this.setCurrentResultScanner(rowRequest.getResultScanner(this.getTable()));
                    return this.getCurrentResultScanner().iterator();
                }

                protected void setNextObject(final T nextObject, final boolean fromExceptionCatch) {

                    this.nextObject = nextObject;

                    if (nextObject == null && !fromExceptionCatch && getListeners() != null) {
                        for (final QueryListener<T> listener : getListeners())
                            listener.onQueryComplete();
                    }
                }

                private void setAggregateRecord(final AggregateRecord aggregateRecord) {
                    this.aggregateRecord = aggregateRecord;
                }

                private AggregateRecord getAggregateRecord() throws HBqlException {
                    return this.aggregateRecord;
                }
            };
        }
        catch (HBqlException e) {
            e.printStackTrace();
        }

        return new Iterator<T>() {

            public boolean hasNext() {
                return false;
            }

            public T next() {
                throw new NoSuchElementException();
            }

            public void remove() {

            }
        };
    }
}
