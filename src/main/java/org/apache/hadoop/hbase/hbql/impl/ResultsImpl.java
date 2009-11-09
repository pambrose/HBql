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
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.hbql.client.Connection;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.Query;
import org.apache.hadoop.hbase.hbql.client.QueryListener;
import org.apache.hadoop.hbase.hbql.client.Results;
import org.apache.hadoop.hbase.hbql.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ResultsImpl<T> implements Results<T> {

    private final List<ResultScanner> resultScannerList = Lists.newArrayList();
    private final QueryImpl<T> query;

    ResultsImpl(final Query<T> query) {
        this.query = (QueryImpl<T>)query;
    }

    private Connection getConnection() {
        return this.getQuery().getConnection();
    }

    private QueryImpl<T> getQuery() {
        return this.query;
    }

    private List<ResultScanner> getResultScannerList() {
        return this.resultScannerList;
    }

    private SelectStatement getSelectStatement() {
        return this.getQuery().getSelectStatement();
    }

    private WithArgs getWithArgs() {
        return this.getSelectStatement().getWithArgs();
    }

    private List<QueryListener<T>> getListeners() {
        return this.getQuery().getListeners();
    }

    private List<RowRequest> getRowRequestList() throws HBqlException, IOException {
        return this.getQuery().getRowRequestList();
    }

    public void close() {

        for (final ResultScanner scanner : this.getResultScannerList())
            closeCurrentResultScanner(scanner, false);

        this.getResultScannerList().clear();
    }

    private void closeCurrentResultScanner(final ResultScanner scanner, final boolean removeFromList) {

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

                private AggregateRecord aggregateRecord = null;

                private final HTable table = getConnection().getHTable(getSelectStatement().getSchema().getTableName());
                private final ExpressionTree clientExpressionTree = getWithArgs().getClientExpressionTree();
                private final Iterator<RowRequest> rowRequestIterator = getRowRequestList().iterator();

                private int maxVersions = 0;
                private ResultScanner currentResultScanner = null;
                private Iterator<Result> currentResultIterator = null;

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
                    this.currentResultScanner = currentResultScanner;
                }

                private void setCurrentResultIterator(final Iterator<Result> currentResultIterator) {
                    this.currentResultIterator = currentResultIterator;
                }

                protected T fetchNextObject() throws HBqlException, IOException {

                    final T firstAttemptVal = doFetch();

                    if (firstAttemptVal != null)
                        return firstAttemptVal;

                    // Try one more time to exhaust all scanners
                    final T secondAttemptVal = doFetch();
                    if (secondAttemptVal == null)
                        closeCurrentResultScanner(this.getCurrentResultScanner(), true);

                    return secondAttemptVal;
                }

                @SuppressWarnings("unchecked")
                private T doFetch() throws HBqlException, IOException {

                    if (this.getCurrentResultIterator() == null)
                        this.setCurrentResultIterator(getNextResultScanner());

                    if (this.getCurrentResultIterator() != null) {

                        final HBaseSchema schema = getSelectStatement().getSchema();

                        // This is the heart of the query evaluation
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

                            if (getSelectStatement().isAnAggregateQuery()) {
                                // Evaluate each of the agregate values
                                this.getAggregateRecord().applyValues(result);
                            }
                            else {
                                final T val = (T)schema.newObject(getSelectStatement().getSelectElementList(),
                                                                  this.getMaxVersions(),
                                                                  result);

                                if (getListeners() != null)
                                    for (final QueryListener<T> listener : getListeners())
                                        listener.onEachRow(val);

                                return val;
                            }
                        }
                    }

                    // Reset to get next scanner
                    this.setCurrentResultIterator(null);
                    return null;
                }

                private Iterator<Result> getNextResultScanner() throws IOException {

                    if (this.getRowRequestIterator().hasNext()) {

                        final RowRequest rowRequest = this.getRowRequestIterator().next();

                        this.setMaxVersions(rowRequest.getMaxVersions());

                        // First close previous ResultScanner before reassigning
                        closeCurrentResultScanner(this.getCurrentResultScanner(), true);

                        this.setCurrentResultScanner(rowRequest.getResultScanner(this.getTable()));

                        getResultScannerList().add(this.getCurrentResultScanner());

                        return this.getCurrentResultScanner().iterator();
                    }
                    else {
                        return null;
                    }
                }

                protected void setNextObject(final T nextObject, final boolean fromExceptionCatch) {

                    this.nextObject = nextObject;

                    if (nextObject == null && !fromExceptionCatch && getListeners() != null) {
                        for (final QueryListener<T> listener : getListeners())
                            listener.onQueryComplete();
                    }
                }

                private AggregateRecord getAggregateRecord() throws HBqlException {

                    if (this.aggregateRecord == null) {
                        final HBaseSchema schema = getSelectStatement().getSchema();
                        this.aggregateRecord = schema.newAggregateRecord(getSelectStatement().getSelectElementList());
                    }

                    return this.aggregateRecord;
                }
            };
        }
        catch (HBqlException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
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
