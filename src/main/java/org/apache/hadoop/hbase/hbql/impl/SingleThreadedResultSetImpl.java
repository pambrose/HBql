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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.QueryListener;
import org.apache.hadoop.hbase.hbql.mapping.ResultAccessor;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SingleThreadedResultSetImpl<T> extends HResultSetImpl<T> {

    private final List<ResultScanner> resultScannerList = Lists.newArrayList();

    SingleThreadedResultSetImpl(final Query<T> query) throws HBqlException {
        super(query);
    }

    private List<ResultScanner> getResultScannerList() {
        return this.resultScannerList;
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

                private int maxVersions = 0;
                private ResultScanner currentResultScanner = null;
                private Iterator<Result> currentResultIterator = null;

                private HTableWrapper tableWrapper = getHConnectionImpl().newHTableWrapper(getWithArgs(),
                                                                                           getTableName());
                private final ExpressionTree clientExpressionTree = getWithArgs().getClientExpressionTree();
                private final Iterator<RowRequest> rowRequestIterator = getQuery().getRowRequestList().iterator();
                private AggregateRecord aggregateRecord = AggregateRecord.newAggregateRecord(getSelectStmt());

                // Prime the iterator with the first value
                private T nextObject = this.fetchNextObject();

                private ExpressionTree getClientExpressionTree() {
                    return this.clientExpressionTree;
                }

                private Iterator<RowRequest> getRowRequestIterator() {
                    return this.rowRequestIterator;
                }

                private HTableWrapper getHTableWrapper() {
                    return this.tableWrapper;
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

                    final ResultAccessor resultAccessor = getQuery().getSelectStmt().getResultAccessor();

                    while (this.getCurrentResultIterator() != null || this.getRowRequestIterator().hasNext()) {

                        if (this.getCurrentResultIterator() == null)
                            this.setCurrentResultIterator(getNextResultIterator());

                        while (this.getCurrentResultIterator().hasNext()) {

                            final Result result = this.getCurrentResultIterator().next();

                            try {
                                if (getClientExpressionTree() != null
                                    && !getClientExpressionTree().evaluate(getHConnectionImpl(), result))
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
                                final T val = (T)resultAccessor.newObject(getHConnectionImpl(),
                                                                          getSelectStmt(),
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
                    this.setCurrentResultScanner(rowRequest.getResultScanner(getSelectStmt().getMapping(),
                                                                             getWithArgs(),
                                                                             this.getHTableWrapper().getHTable()));
                    return this.getCurrentResultScanner().iterator();
                }

                protected void setNextObject(final T nextObject, final boolean fromExceptionCatch) {

                    this.nextObject = nextObject;

                    // If the query is finished then clean up.
                    if (this.getNextObject() == null) {
                        try {
                            if (!fromExceptionCatch && getListeners() != null) {
                                for (final QueryListener<T> listener : getListeners())
                                    listener.onQueryComplete();
                            }

                            try {
                                if (this.getHTableWrapper() != null)
                                    this.getHTableWrapper().getHTable().close();
                            }
                            catch (IOException e) {
                                // No op
                                e.printStackTrace();
                            }
                        }
                        finally {
                            // release to table pool
                            if (this.getHTableWrapper() != null)
                                this.getHTableWrapper().releaseHTable();
                            this.tableWrapper = null;
                        }
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


