package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.io.IOException;
import java.util.Iterator;

public abstract class ResultsIterator<T> implements Iterator<T> {

    // Record count keeps track of values that have evaluated as true and returned to user
    private long returnedRecordCount = 0L;

    private final long returnedRecordLimit;

    protected ResultsIterator(final long returnedRecordLimit) {
        this.returnedRecordLimit = returnedRecordLimit;
    }

    protected abstract T fetchNextObject() throws HBqlException, IOException;

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
        catch (IOException e) {
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

    private long getReturnedRecordLimit() {
        return this.returnedRecordLimit;
    }
}
