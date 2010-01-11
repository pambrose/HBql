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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;
import org.apache.hadoop.hbase.hbql.util.CompletionQueue;
import org.apache.hadoop.hbase.hbql.util.NullIterator;

import java.util.Iterator;
import java.util.List;


public class ResultScannerExecutorResultSet<T> extends HResultSetImpl<T, ResultScanner> {

    ResultScannerExecutorResultSet(final Query<T> query, final ResultScannerExecutor executor) throws HBqlException {
        super(query, executor);
    }

    protected void submitWork(final List<RowRequest> rowRequestList) {
        for (final RowRequest rowRequest : rowRequestList) {
            final Runnable job = new Runnable() {
                public void run() {
                    try {
                        final ResultScanner resultScanner = rowRequest.getResultScanner(getMappingContext().getMapping(),
                                                                                        getWithArgs(),
                                                                                        getTableWrapper().getHTable());
                        getCompletionQueueExecutor().putElement(resultScanner);
                    }
                    catch (HBqlException e) {
                        e.printStackTrace();
                    }
                    finally {
                        getCompletionQueueExecutor().putCompletion();
                    }
                }
            };
            this.getCompletionQueueExecutor().submitWorkToThreadPool(job);
        }
    }


    public Iterator<T> iterator() {

        try {
            return new ResultSetIterator<T, ResultScanner>(this) {

                protected boolean moreResultsPending() {
                    return getCompletionQueueExecutor().moreResultsPending();
                }

                protected Iterator<Result> getNextResultIterator() throws HBqlException {
                    final ResultScanner resultScanner;
                    while (true) {
                        final CompletionQueue.Element<ResultScanner> element = getCompletionQueueExecutor().takeElement();
                        if (element.isCompletionToken()) {
                            if (!moreResultsPending()) {
                                resultScanner = null;
                                break;
                            }
                        }
                        else {
                            resultScanner = element.getValue();
                            break;
                        }
                    }

                    setCurrentResultScanner(resultScanner);
                    return (getCurrentResultScanner() == null) ? null : getCurrentResultScanner().iterator();
                }
            };
        }
        catch (HBqlException e) {
            e.printStackTrace();
            getQuery().callOnException(e);
            return new NullIterator<T>();
        }
    }
}