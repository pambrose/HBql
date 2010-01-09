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
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.mapping.ResultAccessor;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ResultSetIterator<T, R> implements Iterator<T> {

    private final HResultSetImpl<T, R> resultSet;
    private Iterator<Result> currentResultIterator = null;
    private volatile T nextObject = null;
    private final AtomicBoolean iteratorComplete = new AtomicBoolean(false);

    protected ResultSetIterator(final HResultSetImpl<T, R> resultSet) throws HBqlException {
        this.resultSet = resultSet;

        // Prime the iterator with the first value
        this.setNextObject(this.fetchNextObject());
    }

    protected abstract boolean moreResultsPending();

    protected abstract Iterator<Result> getNextResultIterator() throws HBqlException;

    private HResultSetImpl<T, R> getResultSet() {
        return this.resultSet;
    }

    protected T getNextObject() {
        return this.nextObject;
    }

    protected void setNextObject(final T nextObject) {
        this.nextObject = nextObject;
    }

    private AtomicBoolean getIteratorComplete() {
        return this.iteratorComplete;
    }

    protected void markIteratorComplete() {
        this.getIteratorComplete().set(true);
    }

    private boolean isIteratorComplete() {
        return this.getIteratorComplete().get();
    }

    protected void iteratorCompleteAction() {
        this.getResultSet().cleanUpAtEndOfIterator();
    }

    public boolean hasNext() {

        if (this.getResultSet() != null && this.getResultSet().returnedRecordLimitMet())
            this.markIteratorComplete();

        if (this.isIteratorComplete())
            this.iteratorCompleteAction();

        return !this.isIteratorComplete();
    }

    public void remove() {

    }

    private Iterator<Result> getCurrentResultIterator() {
        return this.currentResultIterator;
    }

    private void setCurrentResultIterator(final Iterator<Result> currentResultIterator) {
        this.currentResultIterator = currentResultIterator;
    }

    protected void incrementReturnedRecordCount() {
        if (this.getResultSet() != null)
            this.getResultSet().incrementRecordCount();
    }

    protected void setNextObject(final T nextObject, final boolean fromExceptionCatch) {
        this.setNextObject(nextObject);
    }

    public T next() {

        // Save value to return;
        final T retval = this.getNextObject();

        // Now prefetch next value so that hasNext() will be correct
        try {
            final T nextObject = this.fetchNextObject();
            this.setNextObject(nextObject, false);
        }
        catch (HBqlException e) {
            e.printStackTrace();
            this.setNextObject(null, true);
        }

        return retval;
    }

    @SuppressWarnings("unchecked")
    protected T fetchNextObject() throws HBqlException {

        final HResultSetImpl<T, R> rs = this.getResultSet();
        final SelectStatement selectStatement = rs.getSelectStmt();
        final ResultAccessor resultAccessor = selectStatement.getMappingContext().getResultAccessor();

        while (this.getCurrentResultIterator() != null || moreResultsPending()) {

            if (this.getCurrentResultIterator() == null)
                this.setCurrentResultIterator(this.getNextResultIterator());

            while (this.getCurrentResultIterator() != null
                   && this.getCurrentResultIterator().hasNext()) {

                final Result result = this.getCurrentResultIterator().next();

                try {
                    if (rs.getClientExpressionTree() != null
                        && !rs.getClientExpressionTree().evaluate(rs.getHConnectionImpl(), result))
                        continue;
                }
                catch (ResultMissingColumnException e) {
                    continue;
                }
                catch (NullColumnValueException e) {
                    continue;
                }

                this.incrementReturnedRecordCount();

                if (selectStatement.isAnAggregateQuery()) {
                    this.getResultSet().getAggregateRecord().applyValues(result);
                }
                else {
                    final T val = (T)resultAccessor.newObject(rs.getHConnectionImpl(),
                                                              selectStatement.getMappingContext(),
                                                              selectStatement.getSelectElementList(),
                                                              rs.getMaxVersions(),
                                                              result);

                    return rs.getQuery().callOnEachRow(val);
                }
            }

            this.setCurrentResultIterator(null);
            this.getResultSet().closeResultScanner(this.getResultSet().getCurrentResultScanner(), true);
        }

        if (this.getResultSet().getSelectStmt().isAnAggregateQuery()
            && this.getResultSet().getAggregateRecord() != null) {

            // Stash the value and then null it out for next time through
            final AggregateRecord retval = this.getResultSet().getAggregateRecord();
            this.getResultSet().setAggregateRecord(null);

            return rs.getQuery().callOnEachRow((T)retval);
        }

        this.markIteratorComplete();
        return null;
    }
}
