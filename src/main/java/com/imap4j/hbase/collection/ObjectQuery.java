package com.imap4j.hbase.collection;

import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.schema.ObjectSchema;
import com.imap4j.hbase.util.ResultsIterator;

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
            return new ResultsIterator<T>() {

                final ExprTree exprTree = getExprTree();
                Iterator<T> iter;

                // Prime the iterator with the first value
                T nextObject = fetchNextObject();

                protected T fetchNextObject() throws HPersistException {

                    if (iter == null)
                        iter = getObjects().iterator();

                    while (this.iter.hasNext()) {
                        final T val = this.iter.next();
                        if (this.exprTree.evaluate(val))
                            return val;
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