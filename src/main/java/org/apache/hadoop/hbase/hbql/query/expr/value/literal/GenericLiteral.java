package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:27:29 PM
 */
public abstract class GenericLiteral implements ValueExpr {

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {
        return this;
    }

    @Override
    public boolean isAConstant() throws HBqlException {
        return true;
    }

    @Override
    public void setContext(final ExprTree context) {
    }
}
