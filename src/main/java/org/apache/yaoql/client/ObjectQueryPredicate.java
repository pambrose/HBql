package org.apache.yaoql.client;

import com.google.common.base.Predicate;
import org.apache.expreval.antlr.HBql;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.object.impl.ParameterBinding;
import org.apache.expreval.schema.ReflectionSchema;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

public class ObjectQueryPredicate<T> extends ParameterBinding implements Predicate<T> {

    private final String query;
    private ExpressionTree exprTree;
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
