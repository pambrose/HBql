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
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;
import org.apache.hadoop.hbase.hbql.util.NullIterator;

import java.util.Iterator;
import java.util.List;

public class NonExecutorResultSet<T> extends HResultSetImpl<T, Object> {

    private final Iterator<RowRequest> rowRequestIterator;

    NonExecutorResultSet(final Query<T> query) throws HBqlException {
        super(query, null);
        this.rowRequestIterator = getQuery().getRowRequestList().iterator();
    }

    private Iterator<RowRequest> getRowRequestIterator() {
        return this.rowRequestIterator;
    }

    protected void submitWork(final List<RowRequest> rowRequestList) {
        // No op
    }

    public Iterator<T> iterator() {

        try {
            return new ResultSetIterator<T, Object>(this) {

                protected boolean moreResultsPending() {
                    return getRowRequestIterator().hasNext();
                }

                protected Iterator<Result> getNextResultIterator() throws HBqlException {
                    final RowRequest rowRequest = getRowRequestIterator().next();
                    setCurrentResultScanner(rowRequest.getResultScanner(getMappingContext().getMapping(),
                                                                        getWithArgs(),
                                                                        getTableWrapper().getHTable()));
                    return getCurrentResultScanner().iterator();
                }
            };
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return new NullIterator<T>();
        }
    }
}


