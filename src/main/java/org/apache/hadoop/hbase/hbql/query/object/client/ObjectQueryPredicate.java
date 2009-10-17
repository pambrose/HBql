package org.apache.hadoop.hbase.hbql.query.object.client;

import com.google.common.base.Predicate;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.impl.object.ParameterBinding;
import org.apache.hadoop.hbase.hbql.query.schema.ReflectionSchema;

public class ObjectQueryPredicate<T> extends ParameterBinding implements Predicate<T> {

    private final String query;
    private ExprTree exprTree;
    private boolean initialized = false;

    public ObjectQueryPredicate(final String query) {
        this.query = query;
    }

    public void reset() {
        this.initialized = false;
    }

    public String getQuery() {
        return this.query;
    }

    public boolean apply(final T obj) {

        try {
            if (!initialized) {
                final ReflectionSchema schema = ReflectionSchema.getReflectionSchema(obj);
                this.exprTree = HBql.parseWhereExpression(this.query, schema);
                this.applyParameters(this.exprTree);
                initialized = true;
            }

            return exprTree.evaluate(obj);
        }
        catch (ResultMissingColumnException e) {
            // Not possible
            return false;
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return false;
        }
    }
}
