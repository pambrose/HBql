package org.apache.hadoop.hbase.hbql.query.object;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.util.ResultsIterator;

import java.util.Collection;
import java.util.Iterator;

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

    public Iterator<T> iterator() {

        try {
            return new ResultsIterator<T>() {

                // In theory, this should be done only once and in ObjectQuery, but
                // since it requires the objects to get the schema, I do it here
                final ExprTree exprTree = getObjectQuery().getExprTree(getObjects());

                Iterator<T> objectIter = null;

                // Prime the iterator with the first value
                T nextObject = fetchNextObject();

                protected T fetchNextObject() throws HBqlException {

                    if (objectIter == null)
                        objectIter = getObjects().iterator();

                    while (this.objectIter.hasNext()) {
                        final T val = this.objectIter.next();
                        if (this.exprTree.evaluate(val)) {
                            return val;
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
                return null;
            }

            public void remove() {

            }
        };
    }
}
