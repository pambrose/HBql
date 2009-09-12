package com.imap4j.hbase.collection;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.util.ResultsIterator;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 12, 2009
 * Time: 2:23:26 PM
 */
public class ObjectResults<T> implements Iterable<T> {

    final ObjectQuery<T> objectQuery;
    final Collection<T> objects;

    public ObjectResults(final ObjectQuery objectQuery, final Collection<T> objects) {
        this.objectQuery = objectQuery;
        this.objects = objects;
    }

    private ObjectQuery<T> getObjectQuery() {
        return this.objectQuery;
    }

    private Collection<T> getObjects() {
        return objects;
    }

    @Override
    public Iterator<T> iterator() {

        try {
            return new ResultsIterator<T>() {

                final ExprTree exprTree = getObjectQuery().getExprTree(getObjects());
                Iterator<T> iter;

                // Prime the iterator with the first value
                T nextObject = fetchNextObject();

                protected T fetchNextObject() throws HPersistException {

                    if (iter == null)
                        iter = getObjects().iterator();

                    while (this.iter.hasNext()) {
                        final T val = this.iter.next();
                        if (this.exprTree.evaluate(val)) {
                            return val;
                        }
                    }

                    return null;
                }

                protected T getNextObject() {
                    return this.nextObject;
                }

                protected void setNextObject(final T nextObject) {
                    this.nextObject = nextObject;
                }
            };
        }
        catch (HPersistException e) {
            e.printStackTrace();
        }

        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public T next() {
                return null;
            }

            @Override
            public void remove() {

            }
        };
    }

}
