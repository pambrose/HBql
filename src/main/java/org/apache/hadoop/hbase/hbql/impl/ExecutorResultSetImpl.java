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

import org.apache.expreval.util.NullIterator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.QueryListener;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;


public class ExecutorResultSetImpl<T> extends HResultSetImpl<T> {

    private final Executor executor;
    private volatile boolean closed = false;

    ExecutorResultSetImpl(final Query<T> query) throws HBqlException {
        super(query);
        // This may block waiting for a ExecutorPool to become available
        this.executor = this.getQuery().getHConnectionImpl().getExecutorForConnection();
        // Submit work to executor
        this.submitWork();
    }

    private Executor getExecutor() {
        return this.executor;
    }

    public void close() {
        if (!this.closed) {
            synchronized (this) {
                if (!this.closed) {
                    super.close();
                    this.getExecutor().release();
                    this.closed = true;
                }
            }
        }
    }

    private void submitWork() throws HBqlException {
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
            this.getExecutor().submit(job);
        }
    }

    public Iterator<T> iterator() {

        try {
            return new ResultsIterator<T>(this) {

                protected boolean moreResultsPending() {
                    return getExecutor().moreResultsPending();
                }

                protected Iterator<Result> getNextResultIterator() throws HBqlException {
                    final ResultScanner resultScanner = getExecutor().takeResultScanner();
                    setCurrentResultScanner(resultScanner);
                    return getCurrentResultScanner().iterator();
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
            };
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return new NullIterator<T>();
        }
    }
}