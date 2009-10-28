package org.apache.expreval.hbql.impl;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.io.IOException;
import java.util.Iterator;

public abstract class ResultsIterator<T> implements Iterator<T> {

    protected abstract T fetchNextObject() throws HBqlException, IOException;

    protected abstract T getNextObject();

    protected abstract void setNextObject(final T nextObject, final boolean fromExceptionCatch);

    public T next() {

        // Save value to return;
        final T retval = this.getNextObject();

        // Now prefetch next value so that hasNext() will be correct
        try {
            // Check if queryLimit has been exceeeded

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
}
