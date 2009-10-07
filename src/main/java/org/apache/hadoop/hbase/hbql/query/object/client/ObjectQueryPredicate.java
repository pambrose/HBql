package org.apache.hadoop.hbase.hbql.query.object.client;

import com.google.common.base.Predicate;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.ReflectionSchema;

public class ObjectQueryPredicate<T> implements Predicate<T> {

    private final String query;
    private ExprTree tree;
    private boolean initialized = false;

    public ObjectQueryPredicate(final String query) {
        this.query = query;
    }

    public boolean apply(final T obj) {

        try {
            if (!initialized) {
                final ReflectionSchema schema = ReflectionSchema.getReflectionSchema(obj);
                this.tree = HBql.parseWhereExpression(this.query, schema);
                initialized = true;
            }

            return tree.evaluate(obj);
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return false;
        }
    }
}
