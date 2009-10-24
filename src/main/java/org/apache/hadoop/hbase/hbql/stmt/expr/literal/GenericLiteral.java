package org.apache.hadoop.hbase.hbql.stmt.expr.literal;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public abstract class GenericLiteral<T> implements GenericValue {

    private final T value;

    public GenericLiteral(final T value) {
        this.value = value;
    }

    public T getValue(final Object object) {
        return this.value;
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        return this;
    }

    public boolean isAConstant() {
        return true;
    }

    public void reset() {

    }

    public void setExprContext(final ExprContext context) {
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {
        return this.getReturnType();
    }

    protected abstract Class<? extends GenericValue> getReturnType();

    public String asString() {
        return "" + this.getValue(null);
    }
}
