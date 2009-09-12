package com.imap4j.hbase.collection;

import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.schema.ObjectSchema;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class ObjectQuery<T> implements Iterable<T> {

    final String query;
    final ObjectQueryListener<T> listener;
    Collection<T> objects = null;

    private ObjectQuery(final String query, final ObjectQueryListener<T> listener, final Collection<T> objects) {
        this.query = query;
        this.listener = listener;
        this.objects = objects;
    }

    public static <T> ObjectQuery<T> newObjectQuery(final String query, final ObjectQueryListener<T> listener) {
        return new ObjectQuery<T>(query, listener, null);
    }

    public static <T> ObjectQuery<T> newObjectQuery(final String query, final Collection<T> objs) {
        return new ObjectQuery<T>(query, null, objs);
    }

    public String getQuery() {
        return this.query;
    }

    public ObjectQueryListener<T> getListener() {
        return this.listener;
    }

    public void execute(final Collection<T> objs) throws HPersistException {

        this.objects = objs;

        if (this.getObjects() == null || this.getObjects().size() == 0 || this.getListener() == null)
            return;

        final ExprTree tree = this.getExprTree();

        for (final Object o : objs)
            if (tree.evaluate(o))
                this.getListener().onEachObject((T)o);

    }

    @Override
    public Iterator<T> iterator() {

        try {
            final ExprTree tree = this.getExprTree();

            return new Iterator<T>() {

                final Iterator<T> iter = getObjects().iterator();

                // Prime the iterator with the first value
                T current = getNextObject();

                private T getNextObject() throws HPersistException {
                    while (iter.hasNext()) {
                        final T val = iter.next();
                        if (tree.evaluate(val))
                            return val;
                    }
                    return null;
                }


                @Override
                public T next() {
                    final T retval = current;

                    try {
                        current = getNextObject();
                    }
                    catch (HPersistException e) {
                        e.printStackTrace();
                        current = null;
                    }

                    return retval;
                }

                @Override
                public boolean hasNext() {
                    return current != null;
                }

                @Override
                public void remove() {

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


    private Collection<T> getObjects() {
        return objects;
    }

    private ExprTree getExprTree() throws HPersistException {
        final Object obj = this.getObjects().iterator().next();
        final ObjectSchema schema = ObjectSchema.getObjectSchema(obj);
        final ExprTree tree = (ExprTree)HBqlRule.NODESC_WHERE_EXPR.parse(this.query, schema);
        tree.setSchema(schema);
        tree.optimize();
        return tree;

    }

}