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


public class ResultExecutorResultSet<T> extends HResultSetImpl<T, Result> {

    ResultExecutorResultSet(final Query<T> query, final ResultExecutor executor) throws HBqlException {
        super(query, executor);
    }

    protected void submitWork() throws HBqlException {
        final List<RowRequest> rowRequestList = this.getQuery().getRowRequestList();
        for (final RowRequest rowRequest : rowRequestList) {
            final Callable<String> job = new Callable<String>() {
                public String call() throws HBqlException, InterruptedException {
                    try {
                        final ResultScanner scanner = rowRequest.getResultScanner(getSelectStmt().getMapping(),
                                                                                  getWithArgs(),
                                                                                  getHTableWrapper().getHTable());
                        for (final Result result : scanner) {

                            try {
                                if (getClientExpressionTree() != null
                                    && !getClientExpressionTree().evaluate(getHConnectionImpl(), result))
                                    continue;
                            }
                            catch (ResultMissingColumnException e) {
                                continue;
                            }

                            getExecutor().getQueue().putElement(result);
                        }

                        scanner.close();
                    }
                    finally {
                        getExecutor().getQueue().putCompletion();
                    }
                    return "OK";
                }
            };
            this.getExecutor().submit(job);
        }
    }

    public Iterator<T> iterator() {

        try {
            return new ResultSetIterator<T, Result>(this) {

                protected Iterator<Result> getNextResultIterator() throws HBqlException {
                    return null;
                }

                protected void cleanUp(final boolean fromExceptionCatch) {
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

                protected boolean moreResultsPending() {
                    return getExecutor().moreResultsPending(getExecutor().getQueue().getCompletionCount());
                }

                @SuppressWarnings("unchecked")
                protected T fetchNextObject() throws HBqlException {

                    final ResultAccessor resultAccessor = getSelectStmt().getResultAccessor();

                    // Read data until all jobs have sent DONE tokens
                    while (true) {
                        final Result result;
                        try {
                            final QueueElement<Result> queueElement = getExecutor().getQueue().takeElement();
                            if (queueElement.isCompleteToken()) {
                                if (!moreResultsPending())
                                    break;
                                else
                                    continue;
                            }
                            else {
                                result = queueElement.getElement();
                            }
                        }
                        catch (InterruptedException e) {
                            throw new HBqlException(e);
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

                    if (getSelectStmt().isAnAggregateQuery() && getAggregateRecord() != null) {
                        // Stash the value and then null it out for next time through
                        final AggregateRecord retval = getAggregateRecord();
                        setAggregateRecord(null);
                        return (T)retval;
                    }

                    return null;
                }
            };
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return new NullIterator<T>();
        }
    }
}