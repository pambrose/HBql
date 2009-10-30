package org.apache.yaoql.client;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.hadoop.hbase.contrib.hbql.impl.ResultsIterator;
import org.apache.yaoql.impl.ObjectQueryImpl;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ObjectResults<T> implements Iterable<T> {

    final ObjectQueryImpl<T> objectQuery;
    final Collection<T> objects;

    public ObjectResults(final ObjectQueryImpl objectQuery, final Collection<T> objects) {
        this.objectQuery = objectQuery;
        this.objects = objects;
    }

    private ObjectQueryImpl<T> getObjectQuery() {
        return this.objectQuery;
    }

    private Collection<T> getObjects() {
        return objects;
    }

    public Iterator<T> iterator() {

        try {
            return new ResultsIterator<T>() {

                // In theory, this should be done only once and in ObjectQuery, but
                // since it requires the objects to get the schema, I do it here
                final ExpressionTree expressionTree = getObjectQuery().getExpressionTree(getObjects());

                Iterator<T> objectIter = null;

                // Prime the iterator with the first value
                T nextObject = fetchNextObject();

                protected T fetchNextObject() throws HBqlException {

                    if (objectIter == null)
                        objectIter = getObjects().iterator();

                    while (this.objectIter.hasNext()) {
                        final T val = this.objectIter.next();
                        try {
                            if (this.expressionTree.evaluate(val))
                                return val;
                        }
                        catch (ResultMissingColumnException e) {
                            // Just skip and do nothing
                        }
                    }

                    return null;
                }

                protected T getNextObject() {
                    return this.nextObject;
                }

                protected void setNextObject(final T nextObject, final boolean fromExceptionCatch) {
                    this.nextObject = nextObject;
                }
            };
        }
        catch (HBqlException e) {
            e.printStackTrace();
        }

        return new Iterator<T>() {

            public boolean hasNext() {
                return false;
            }

            public T next() {
                throw new NoSuchElementException();
            }

            public void remove() {

            }
        };
    }
}
