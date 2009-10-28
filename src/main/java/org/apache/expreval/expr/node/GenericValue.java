package org.apache.expreval.expr.node;

import org.apache.expreval.expr.ExpressionContext;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

import java.io.Serializable;

public interface GenericValue extends Serializable {

    void setExprContext(final ExpressionContext context) throws HBqlException;

    Object getValue(final Object object) throws HBqlException, ResultMissingColumnException;

    GenericValue getOptimizedValue() throws HBqlException;

    Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                final boolean allowsCollections) throws HBqlException;

    boolean isAConstant();

    boolean isDefaultKeyword();

    boolean hasAColumnReference();

    String asString();

    void reset();
}
