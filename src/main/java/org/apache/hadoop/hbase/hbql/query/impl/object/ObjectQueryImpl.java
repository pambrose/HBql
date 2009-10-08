package org.apache.hadoop.hbase.hbql.query.impl.object;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.object.client.ObjectQuery;
import org.apache.hadoop.hbase.hbql.query.object.client.ObjectQueryListener;
import org.apache.hadoop.hbase.hbql.query.object.client.ObjectResults;
import org.apache.hadoop.hbase.hbql.query.schema.ReflectionSchema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.Collection;
import java.util.List;

public class ObjectQueryImpl<T> extends ParameterBinding implements ObjectQuery<T> {

    final String query;
    List<ObjectQueryListener<T>> listeners = null;

    public ObjectQueryImpl(final String query) {
        this.query = query;
    }

    public void addListener(final ObjectQueryListener<T> listener) {
        if (this.getListeners() == null)
            this.listeners = Lists.newArrayList();

        this.getListeners().add(listener);
    }

    private List<ObjectQueryListener<T>> getListeners() {
        return this.listeners;
    }

    public void clearListeners() {
        if (this.getListeners() != null)
            this.getListeners().clear();
    }

    public String getQuery() {
        return this.query;
    }

    public ExprTree getExprTree(final Collection<T> objects) throws HBqlException {

        if (objects == null || objects.size() == 0) {
            ExprTree exprTree = ExprTree.newExprTree(true);
            exprTree.setSchema(null);
            return exprTree;
        }

        // Grab the first object to derive the schema
        final Object obj = objects.iterator().next();
        final ReflectionSchema schema = ReflectionSchema.getReflectionSchema(obj);
        final ExprTree exprTree = HBql.parseWhereExpression(this.getQuery(), schema);
        this.applyParameters(exprTree);
        return exprTree;
    }

    public ObjectResults<T> getResults(final Collection<T> objs) throws HBqlException {

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

    public List<T> getResultList(final Collection<T> objs) throws HBqlException {

        final List<T> retval = Lists.newArrayList();

        final ObjectResults<T> results = this.getResults(objs);

        for (T val : results)
            retval.add(val);

        return retval;
    }
}