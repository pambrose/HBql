package com.imap4j.hbase.collection;

import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ReflectionEvalContext;
import com.imap4j.hbase.hbql.expr.predicate.ExprEvalTree;
import com.imap4j.hbase.hbql.schema.ObjectSchema;

import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class CollectionQuery<T> {

    final String query;
    final CollectionQueryListener<T> listener;

    public CollectionQuery(final String query, final CollectionQueryListener<T> listener) {
        this.query = query;
        this.listener = listener;
    }

    public String getQuery() {
        return this.query;
    }

    public CollectionQueryListener<T> getListener() {
        return this.listener;
    }

    public void execute(final Collection<T> objs) throws HPersistException {

        if (objs == null || objs.size() == 0)
            return;

        final Object obj = objs.iterator().next();
        final ObjectSchema schema = ObjectSchema.getObjectSchema(obj);
        final ExprEvalTree tree = (ExprEvalTree)HBqlRule.NODESC_WHERE_EXPR.parse(this.query, schema);
        tree.setSchema(schema);
        tree.optimize();

        for (final Object o : objs)
            if (tree.evaluate(new ReflectionEvalContext(o)))
                this.getListener().onEachObject((T)o);

    }
}