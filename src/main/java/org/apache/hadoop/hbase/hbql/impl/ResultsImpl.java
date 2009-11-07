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

    private final List<ResultScanner> scannerList = Lists.newArrayList();
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

    private List<ResultScanner> getScannerList() {
        return this.scannerList;
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
        for (final ResultScanner scanner : this.getScannerList())
            closeCurrentScanner(scanner, false);

        this.scannerList.clear();
    }

    private void closeCurrentScanner(final ResultScanner scanner, final boolean removeFromList) {

        if (scanner != null) {
            try {
                scanner.close();
            }
            catch (Exception e) {
                // Do nothing
            }

            if (removeFromList)
                getScannerList().remove(scanner);
        }
    }

    public Iterator<T> iterator() {

        try {
            return new ResultsIterator<T>(this.getWithArgs().getLimit()) {

                private final HTable table = getConnection().getHTable(getSelectStatement().getSchema().getTableName());
                private final ExpressionTree clientExpressionTree = getWithArgs().getClientExpressionTree();
                private final Iterator<RowRequest> rowRequestIter = getRowRequestList().iterator();

                private int maxVersions = 0;
                private ResultScanner currentResultScanner = null;
                private Iterator<Result> resultIter = null;

                // Prime the iterator with the first value
                private T nextObject = fetchNextObject();

                private ExpressionTree getClientExpressionTree() {
                    return this.clientExpressionTree;
                }

                private Iterator<RowRequest> getRowRequestIter() {
                    return this.rowRequestIter;
                }

                private int getMaxVersions() {
                    return this.maxVersions;
                }

                private ResultScanner getCurrentResultScanner() {
                    return this.currentResultScanner;
                }

                private Iterator<Result> getResultIter() {
                    return this.resultIter;
                }

                protected T getNextObject() {
                    return this.nextObject;
                }

                private HTable getTable() {
                    return this.table;
                }

                private Iterator<Result> getNextResultScanner() throws IOException {
                    if (this.getRowRequestIter().hasNext()) {

                        final RowRequest rowRequest = this.getRowRequestIter().next();

                        this.maxVersions = rowRequest.getMaxVersions();

                        // First close previous ResultScanner before reassigning
                        closeCurrentScanner(this.getCurrentResultScanner(), true);

                        this.currentResultScanner = rowRequest.getResultScanner(this.getTable());

                        getScannerList().add(this.getCurrentResultScanner());

                        return this.getCurrentResultScanner().iterator();
                    }
                    else {
                        return null;
                    }
                }

                protected T fetchNextObject() throws HBqlException, IOException {

                    final T firstAttemptVal = doFetch();

                    if (firstAttemptVal != null)
                        return firstAttemptVal;

                    // Try one more time
                    final T secondAttemptVal = doFetch();
                    if (secondAttemptVal == null)
                        closeCurrentScanner(this.getCurrentResultScanner(), true);

                    return secondAttemptVal;
                }

                @SuppressWarnings("unchecked")
                private T doFetch() throws HBqlException, IOException {

                    if (this.getResultIter() == null)
                        this.resultIter = getNextResultScanner();

                    if (this.getResultIter() != null) {

                        final HBaseSchema schema = getSelectStatement().getSchema();

                        while (this.getResultIter().hasNext()) {

                            final Result result = this.getResultIter().next();

                            try {
                                if (getClientExpressionTree() == null || getClientExpressionTree().evaluate(result)) {

                                    incrementReturnedRecordCount();

                                    final T val = (T)schema.newObject(getSelectStatement().getSelectElementList(),
                                                                      this.getMaxVersions(),
                                                                      result);

                                    if (getListeners() != null)
                                        for (final QueryListener<T> listener : getListeners())
                                            listener.onEachRow(val);

                                    return val;
                                }
                            }
                            catch (ResultMissingColumnException e) {
                                // Just skip and do nothing
                            }
                        }
                    }

                    // Reset to get next scanner
                    this.resultIter = null;
                    return null;
                }

                protected void setNextObject(final T nextObject, final boolean fromExceptionCatch) {

                    this.nextObject = nextObject;

                    if (nextObject == null && !fromExceptionCatch && getListeners() != null) {
                        for (final QueryListener<T> listener : getListeners())
                            listener.onQueryComplete();
                    }
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
