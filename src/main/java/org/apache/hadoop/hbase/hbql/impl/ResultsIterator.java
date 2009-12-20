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

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.Iterator;

public abstract class ResultsIterator<T> implements Iterator<T> {

    // Record count keeps track of values that have evaluated as true and returned to user
    private long returnedRecordCount = 0L;

    private final long returnedRecordLimit;

    protected ResultsIterator(final long returnedRecordLimit) {
        this.returnedRecordLimit = returnedRecordLimit;
    }

    protected abstract T fetchNextObject() throws HBqlException;

    protected abstract T getNextObject();

    protected abstract void setNextObject(final T nextObject, final boolean fromExceptionCatch);

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

    public boolean hasNext() {
        return this.getNextObject() != null;
    }

    public void remove() {

    }

    private boolean returnedRecordLimitMet() {
        return this.getReturnedRecordLimit() > 0 && this.getReturnedRecordCount() >= this.getReturnedRecordLimit();
    }

    private long getReturnedRecordLimit() {
        return this.returnedRecordLimit;
    }

    private long getReturnedRecordCount() {
        return this.returnedRecordCount;
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
}
