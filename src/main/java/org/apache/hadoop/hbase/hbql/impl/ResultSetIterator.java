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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.QueryListener;
import org.apache.hadoop.hbase.hbql.mapping.ResultAccessor;

import java.util.Iterator;

public abstract class ResultSetIterator<T> implements Iterator<T> {

    // Record count keeps track of values that have evaluated as true and returned to user
    private long returnedRecordCount = 0L;

    private final HResultSetImpl<T> resultSet;
    private final long returnedRecordLimit;
    private Iterator<Result> currentResultIterator = null;
    private T nextObject = null;
    private AggregateRecord aggregateRecord;

    protected ResultSetIterator(final HResultSetImpl<T> resultSet) throws HBqlException {
        this.resultSet = resultSet;

        if (this.getResultSet() != null) {
            this.returnedRecordLimit = this.getResultSet().getWithArgs().getLimit();
            this.setAggregateRecord(AggregateRecord.newAggregateRecord(resultSet.getQuery().getSelectStmt()));
        }
        else {
            this.returnedRecordLimit = -1L;
        }

        // Prime the iterator with the first value
        this.setNextObject(this.fetchNextObject());
    }

    protected abstract void cleanUp(final boolean fromExceptionCatch);

    protected abstract boolean moreResultsPending();

    protected abstract Iterator<Result> getNextResultIterator() throws HBqlException;

    private HResultSetImpl<T> getResultSet() {
        return this.resultSet;
    }

    protected T getNextObject() {
        return this.nextObject;
    }

    protected void setNextObject(final T nextObject) {
        this.nextObject = nextObject;
    }

    public boolean hasNext() {
        return this.getNextObject() != null;
    }

    public void remove() {

    }

    private boolean returnedRecordLimitMet() {
        return this.getReturnedRecordLimit() > 0
               && this.getReturnedRecordCount() >= this.getReturnedRecordLimit();
    }

    private long getReturnedRecordLimit() {
        return this.returnedRecordLimit;
    }

    private long getReturnedRecordCount() {
        return this.returnedRecordCount;
    }

    private Iterator<Result> getCurrentResultIterator() {
        return this.currentResultIterator;
    }

    private void setCurrentResultIterator(final Iterator<Result> currentResultIterator) {
        this.currentResultIterator = currentResultIterator;
    }

    private void setAggregateRecord(final AggregateRecord aggregateRecord) {
        this.aggregateRecord = aggregateRecord;
    }

    private AggregateRecord getAggregateRecord() throws HBqlException {
        return this.aggregateRecord;
    }

    protected void incrementReturnedRecordCount() {
        this.returnedRecordCount++;

        // See if the limit has been met.  If so, then advance through the rest of the results
        if (this.returnedRecordLimitMet()) {
            while (this.hasNext()) {
                this.next();
            }
        }
    }

    protected void setNextObject(final T nextObject, final boolean fromExceptionCatch) {

        this.setNextObject(nextObject);

        // If the query is finished then clean up.
        if (!this.hasNext())
            this.cleanUp(fromExceptionCatch);
    }

    public T next() {

        // Save value to return;
        final T retval = this.getNextObject();

        // Now prefetch next value so that hasNext() will be correct
        try {
            this.setNextObject(this.fetchNextObject(), false);
        }
        catch (HBqlException e) {
            e.printStackTrace();
            this.setNextObject(null, true);
        }

        return retval;
    }

    @SuppressWarnings("unchecked")
    protected T fetchNextObject() throws HBqlException {

        final ResultAccessor resultAccessor = this.getResultSet().getQuery().getSelectStmt().getResultAccessor();

        while (this.getCurrentResultIterator() != null || moreResultsPending()) {

            if (this.getCurrentResultIterator() == null)
                this.setCurrentResultIterator(getNextResultIterator());

            while (this.getCurrentResultIterator().hasNext()) {

                final Result result = this.getCurrentResultIterator().next();

                try {
                    if (this.getResultSet().getClientExpressionTree() != null
                        && !this.getResultSet()
                            .getClientExpressionTree()
                            .evaluate(this.getResultSet().getHConnectionImpl(), result))
                        continue;
                }
                catch (ResultMissingColumnException e) {
                    continue;
                }

                incrementReturnedRecordCount();

                if (this.getResultSet().getSelectStmt().isAnAggregateQuery()) {
                    this.getAggregateRecord().applyValues(result);
                }
                else {
                    final T val = (T)resultAccessor.newObject(this.getResultSet().getHConnectionImpl(),
                                                              this.getResultSet().getSelectStmt(),
                                                              this.getResultSet()
                                                                      .getSelectStmt().getSelectElementList(),
                                                              this.getResultSet().getMaxVersions(),
                                                              result);

                    if (this.getResultSet().getListeners() != null)
                        for (final QueryListener<T> listener : this.getResultSet().getListeners())
                            listener.onEachRow(val);

                    return val;
                }
            }

            this.setCurrentResultIterator(null);
            this.getResultSet().closeResultScanner(this.getResultSet().getCurrentResultScanner(), true);
        }

        if (this.getResultSet().getSelectStmt().isAnAggregateQuery()
            && this.getAggregateRecord() != null) {
            // Stash the value and then null it out for next time through
            final AggregateRecord retval = this.getAggregateRecord();
            this.setAggregateRecord(null);
            return (T)retval;
        }

        return null;
    }
}
