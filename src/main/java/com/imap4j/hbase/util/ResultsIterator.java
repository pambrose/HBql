package com.imap4j.hbase.util;

import com.imap4j.hbase.hbase.HPersistException;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 12, 2009
 * Time: 12:30:57 PM
 */
public abstract class ResultsIterator<T> implements Iterator<T> {

    protected abstract T fetchNextObject() throws HPersistException, IOException;

    protected abstract T getNextObject();

    protected abstract void setNextObject(final T nextObject, final boolean fromExceptionCatch);

    @Override
    public T next() {

        // Save value to return;
        final T retval = this.getNextObject();

        // Now prefetch next value so that hasNext() will be correct
        try {
            this.setNextObject(fetchNextObject(), false);
        }
        catch (HPersistException e) {
            e.printStackTrace();
            this.setNextObject(null, true);
        }
        catch (IOException e) {
            e.printStackTrace();
            this.setNextObject(null, true);
        }

        return retval;
    }

    @Override
    public boolean hasNext() {
        return this.getNextObject() != null;
    }

    @Override
    public void remove() {

    }
}
