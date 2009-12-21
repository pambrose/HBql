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
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.NullIterator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.QueryListener;
import org.apache.hadoop.hbase.hbql.mapping.ResultAccessor;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;


public class ThreadPoolResultSetImpl<T> extends HResultSetImpl<T> {

    private final QueryService queryService;
    private final List<ResultScanner> resultScannerList = Lists.newArrayList();

    private ResultScanner currentResultScanner = null;
    private Iterator<Result> currentResultIterator = null;

    ThreadPoolResultSetImpl(final Query<T> query) throws HBqlException {
        super(query);

        // This may block waiting for a ExecutorService to become available
        this.queryService = ThreadPoolManager.getThreadPool(getThreadPoolName()).take();

        // Submit work to executor completion service
        final List<RowRequest> rowRequestList = this.getQuery().getRowRequestList();
        for (final RowRequest rowRequest : rowRequestList) {
            final Callable<ResultScanner> job = new Callable<ResultScanner>() {
                public ResultScanner call() {
                    try {
                        setMaxVersions(rowRequest.getMaxVersions());
                        return rowRequest.getResultScanner(getSelectStmt().getMapping(),
                                                           getWithArgs(),
                                                           getHTableWrapper().getHTable());
                    }
                    catch (HBqlException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            };

            this.getQueryService().submit(job);
        }
    }

    private QueryService getQueryService() {
        return this.queryService;
    }

    private List<ResultScanner> getResultScannerList() {
        return this.resultScannerList;
    }

    private String getThreadPoolName() {
        return this.getQuery().getSelectStmt().getWithArgs().getKeyRangeArgs().getThreadPoolName();
    }

    private ResultScanner getCurrentResultScanner() {
        return this.currentResultScanner;
    }

    private Iterator<Result> getCurrentResultIterator() {
        return this.currentResultIterator;
    }

    private void setCurrentResultScanner(final ResultScanner currentResultScanner) {
        // First close previous ResultScanner before reassigning
        closeResultScanner(this.getCurrentResultScanner(), true);
        this.currentResultScanner = currentResultScanner;
        this.getResultScannerList().add(this.getCurrentResultScanner());
    }

    private void setCurrentResultIterator(final Iterator<Result> currentResultIterator) {
        this.currentResultIterator = currentResultIterator;
    }

    public void close() {
        for (final ResultScanner scanner : this.getResultScannerList())
            closeResultScanner(scanner, false);

        this.getResultScannerList().clear();

        this.getQueryService().release();
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

                @SuppressWarnings("unchecked")
                protected T fetchNextObject() throws HBqlException {

                    final ResultAccessor resultAccessor = getQuery().getSelectStmt().getResultAccessor();

                    while (getCurrentResultIterator() != null || getQueryService().moreResultsPending()) {

                        if (getCurrentResultIterator() == null)
                            setCurrentResultIterator(getNextResultIterator());

                        while (getCurrentResultIterator().hasNext()) {

                            final Result result = getCurrentResultIterator().next();

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
                                getAggregateRecord().applyValues(result);
                            }
                            else {
                                final T val = (T)resultAccessor.newObject(getHConnectionImpl(),
                                                                          getSelectStmt(),
                                                                          getSelectStmt().getSelectElementList(),
                                                                          getMaxVersions(),
                                                                          result);

                                if (getListeners() != null)
                                    for (final QueryListener<T> listener : getListeners())
                                        listener.onEachRow(val);

                                return val;
                            }
                        }

                        setCurrentResultIterator(null);
                        closeResultScanner(getCurrentResultScanner(), true);
                    }

                    if (getSelectStmt().isAnAggregateQuery() && getAggregateRecord() != null) {
                        // Stash the value and then null it out for next time through
                        final AggregateRecord retval = getAggregateRecord();
                        setAggregateRecord(null);
                        return (T)retval;
                    }

                    return null;
                }

                private Iterator<Result> getNextResultIterator() throws HBqlException {
                    try {
                        final Future<ResultScanner> future = getQueryService().take();
                        final ResultScanner resultScanner = future.get();
                        setCurrentResultScanner(resultScanner);
                        return getCurrentResultScanner().iterator();
                    }
                    catch (InterruptedException e) {
                        throw new HBqlException(e);
                    }
                    catch (java.util.concurrent.ExecutionException e) {
                        throw new HBqlException(e);
                    }
                }

                protected void setNextObject(final T nextObject, final boolean fromExceptionCatch) {

                    this.setNextObject(nextObject);

                    // If the query is finished then clean up.
                    if (!this.hasNext()) {
                        try {
                            if (!fromExceptionCatch && getListeners() != null) {
                                for (final QueryListener<T> listener : getListeners())
                                    listener.onQueryComplete();
                            }

                            try {
                                if (getHTableWrapper() != null)
                                    getHTableWrapper().getHTable().close();
                            }
                            catch (IOException e) {
                                // No op
                                e.printStackTrace();
                            }
                        }
                        finally {
                            // release to table pool
                            if (getHTableWrapper() != null)
                                getHTableWrapper().releaseHTable();
                            setTableWrapper(null);

                            close();
                        }
                    }
                }
            };
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return new NullIterator<T>();
        }
    }
}