package org.apache.hadoop.hbase.hbql.query.object;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.ReflectionSchema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.Collection;
import java.util.List;

public class ObjectQuery<T> {

    final String query;
    List<ObjectQueryListener<T>> listeners = null;

    private ObjectQuery(final String query) {
        this.query = query;
    }

    public void addListener(final ObjectQueryListener<T> listener) {
        if (this.getListeners() == null)
            this.listeners = listeners = Lists.newArrayList();

        this.getListeners().add(listener);
    }

    private List<ObjectQueryListener<T>> getListeners() {
        return this.listeners;
    }

    public void clearListeners() {
        if (this.getListeners() != null)
            this.getListeners().clear();
    }

    public static <T> ObjectQuery<T> newObjectQuery(final String query) {
        return new ObjectQuery<T>(query);
    }

    public String getQuery() {
        return this.query;
    }

    ExprTree getExprTree(final Collection<T> objects) throws HBqlException {
        final Object obj = objects.iterator().next();
        final ReflectionSchema schema = ReflectionSchema.getReflectionSchema(obj);
        return HBql.parseWhereExpression(this.getQuery(), schema);
    }

    public ObjectResults<T> execute(final Collection<T> objs) throws HBqlException {

        final ObjectResults<T> retval = new ObjectResults<T>(this, objs);

        if (this.getListeners() != null && this.getListeners().size() > 0) {

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