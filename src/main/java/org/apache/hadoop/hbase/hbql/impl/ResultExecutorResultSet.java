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

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.mapping.ResultAccessor;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;
import org.apache.hadoop.hbase.hbql.util.CompletionQueue;
import org.apache.hadoop.hbase.hbql.util.NullIterator;

import java.util.Iterator;
import java.util.List;


public class ResultExecutorResultSet<T> extends HResultSetImpl<T, Result> {

    ResultExecutorResultSet(final Query<T> query, final ResultExecutor executor) throws HBqlException {
        super(query, executor);
    }

    protected void submitWork(final List<RowRequest> rowRequestList) {

        // This will submit jobs until a job is rejected, at which point, execution will take place in
        // the context of this thread
        for (final RowRequest rowRequest : rowRequestList) {
            this.getCompletionQueueExecutor().submitWorkToThreadPool(new Runnable() {
                public void run() {
                    try {
                        final ResultScanner scanner = rowRequest.getResultScanner(getMappingContext().getMapping(),
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
                            catch (NullColumnValueException e) {
                                continue;
                            }

                            getCompletionQueueExecutor().putElement(result);
                        }

                        scanner.close();
                    }

                    catch (HBqlException e) {
                        e.printStackTrace();
                    }
                    finally {
                        getCompletionQueueExecutor().putCompletion();
                    }
                }
            });
        }
    }

    public Iterator<T> iterator() {

        try {
            return new ResultSetIterator<T, Result>(this) {

                protected boolean moreResultsPending() {
                    return getCompletionQueueExecutor().moreResultsPending();
                }

                protected Iterator<Result> getNextResultIterator() throws HBqlException {
                    return null;
                }

                @SuppressWarnings("unchecked")
                protected T fetchNextObject() throws HBqlException {

                    final ResultAccessor resultAccessor = getMappingContext().getResultAccessor();

                    // Read data until all jobs have sent DONE tokens
                    while (true) {
                        final Result result;
                        final CompletionQueue.Element<Result> element = getCompletionQueueExecutor().takeElement();
                        if (element.isCompletionToken()) {
                            if (!moreResultsPending())
                                break;
                            else
                                continue;
                        }
                        else {
                            result = element.getValue();
                        }

                        this.incrementReturnedRecordCount();

                        if (getSelectStmt().isAnAggregateQuery()) {
                            getAggregateRecord().applyValues(result);
                        }
                        else {
                            final T val = (T)resultAccessor.newObject(getHConnectionImpl(),
                                                                      getMappingContext(),
                                                                      getSelectStmt().getSelectElementList(),
                                                                      getMaxVersions(),
                                                                      result);
                            return getQuery().callOnEachRow(val);
                        }
                    }

                    if (getSelectStmt().isAnAggregateQuery() && getAggregateRecord() != null) {

                        // Stash the value and then null it out for next time through
                        final AggregateRecord retval = getAggregateRecord();
                        setAggregateRecord(null);

                        return getQuery().callOnEachRow((T)retval);
                    }

                    this.getIteratorComplete().set(true);
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