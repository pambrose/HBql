package com.imap4j.hbase.collection;

import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.schema.ObjectSchema;
import com.imap4j.hbase.util.Lists;

import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class ObjectQuery<T> {

    final String query;
    final List<ObjectQueryListener<T>> listeners = Lists.newArrayList();

    private ObjectQuery(final String query) {
        this.query = query;
    }

    public void addListener(final ObjectQueryListener<T> listener) {
        this.getListeners().add(listener);
    }

    public List<ObjectQueryListener<T>> getListeners() {
        return listeners;
    }

    public static <T> ObjectQuery<T> newObjectQuery(final String query) {
        return new ObjectQuery<T>(query);
    }

    public String getQuery() {
        return this.query;
    }

    ExprTree getExprTree(final Collection<T> objects) throws HPersistException {
        final Object obj = objects.iterator().next();
        final ObjectSchema schema = ObjectSchema.getObjectSchema(obj);
        final ExprTree tree = (ExprTree)HBqlRule.NODESC_WHERE_EXPR.parse(this.query, schema);
        tree.setSchema(schema);
        tree.optimize();
        return tree;

    }

    public ObjectResults<T> execute(final Collection<T> objs) throws HPersistException {

        final ObjectResults<T> retval = new ObjectResults<T>(this, objs);

        if (this.getListeners().size() > 0) {

            for (final ObjectQueryListener<T> listener : this.getListeners())
                listener.onQueryInit();

            for (final T val : retval) {
                for (final ObjectQueryListener<T> listener : this.getListeners())
                    listener.onEachObject((T)val);
            }

            for (final ObjectQueryListener<T> listener : this.getListeners())
                listener.onQueryComplete();
        }

        return retval;
    }
}