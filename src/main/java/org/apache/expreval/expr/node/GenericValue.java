package org.apache.expreval.expr.node;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.io.Serializable;

public interface GenericValue extends Serializable {

    void setExpressionContext(final MultipleExpressionContext context) throws HBqlException;

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
