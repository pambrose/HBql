package org.apache.expreval.expr.literal;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public class DefaultKeyword implements GenericValue {

    public void setExpressionContext(final MultipleExpressionContext context) throws HBqlException {

    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return null;
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        return null;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr, final boolean allowsCollections) throws HBqlException {
        return DefaultKeyword.class;
    }

    public boolean isAConstant() {
        return true;
    }

    public boolean isDefaultKeyword() {
        return true;
    }

    public boolean hasAColumnReference() {
        return false;
    }

    public String asString() {
        return "DEFAULT";
    }

    public void reset() {

    }
}
