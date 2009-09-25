package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:27:29 PM
 */
public abstract class GenericLiteral<T extends ValueExpr> implements ValueExpr {

    @Override
    public T getOptimizedValue() throws HPersistException {
        return (T)this;
    }

    @Override
    public boolean isAConstant() {
        return true;
    }

    @Override
    public void setContext(final ExprTree context) {
    }

}
