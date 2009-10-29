package org.apache.yaoql.impl;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.contrib.hbql.parser.Parser;
import org.apache.hadoop.hbase.contrib.hbql.schema.ReflectionSchema;
import org.apache.yaoql.client.ObjectQuery;
import org.apache.yaoql.client.ObjectQueryListener;
import org.apache.yaoql.client.ObjectResults;

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

    public ExpressionTree getExpressionTree(final Collection<T> objects) throws HBqlException {

        if (objects == null || objects.size() == 0) {
            final ExpressionTree expressionTree = ExpressionTree.newExpressionTree(true);
            expressionTree.setSchema(null);
            return expressionTree;
        }

        // Grab the first object to derive the schema
        final Object obj = objects.iterator().next();
        final ReflectionSchema schema = ReflectionSchema.getReflectionSchema(obj);
        final ExpressionTree expressionTree = Parser.parseWhereExpression(this.getQuery(), schema);
        this.applyParameters(expressionTree);
        return expressionTree;
    }

    public ObjectResults<T> getResults(final Collection<T> objs) throws HBqlException {

        final ObjectResults<T> retval = new ObjectResults<T>(this, objs);

        if (this.getListeners() != null && this.getListeners().size() > 0) {

            for (final ObjectQueryListener<T> listener : this.getListeners())
                listener.onQueryInit();

            for (final T val : retval) {
                for (final ObjectQueryListener<T> listener : this.getListeners())
                    listener.onEachObject(val);
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