package org.apache.hadoop.hbase.hbql.query.object;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.antlr.config.HBqlRule;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;
import org.apache.hadoop.hbase.hbql.query.schema.ObjectSchema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

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

    ExprTree getExprTree(final Collection<T> objects) throws HPersistException {
        final Object obj = objects.iterator().next();
        final ObjectSchema schema = ObjectSchema.getObjectSchema(obj);
        return HUtil.parseExprTree(HBqlRule.NODESC_WHERE_EXPR, this.query, schema, true);
    }

    public ObjectResults<T> execute(final Collection<T> objs) throws HPersistException {

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