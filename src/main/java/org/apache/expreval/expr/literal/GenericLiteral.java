package org.apache.expreval.expr.literal;

import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

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

    public boolean isDefaultKeyword() {
        return false;
    }

    public boolean hasAColumnReference() {
        return false;
    }

    public void reset() {

    }

    public void setExpressionContext(final MultipleExpressionContext context) {
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
