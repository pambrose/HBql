package org.apache.hadoop.hbase.hbql.query.object;

import com.google.common.base.Predicate;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.antlr.config.HBqlRule;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;
import org.apache.hadoop.hbase.hbql.query.schema.ReflectionSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 9, 2009
 * Time: 10:09:38 PM
 */
public class ObjectQueryPredicate<T> implements Predicate<T> {

    private final String query;
    private ExprTree tree;
    private boolean initialized = false;

    public ObjectQueryPredicate(final String query) {
        this.query = query;
    }

    @Override
    public boolean apply(final T obj) {

        try {
            if (!initialized) {
                final ReflectionSchema schema = ReflectionSchema.getReflectionSchema(obj);
                this.tree = HUtil.parseExprTree(HBqlRule.NODESC_WHERE_EXPR, this.query, schema, true);
                initialized = true;
            }

            return tree.evaluate(obj);
        }
        catch (HPersistException e) {
            e.printStackTrace();
            return false;
        }
    }
}
