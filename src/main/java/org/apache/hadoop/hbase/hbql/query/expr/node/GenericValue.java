package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;

import java.io.Serializable;

public interface GenericValue extends Serializable {

    void setExprContext(final ExprContext context) throws HBqlException;

    Object getValue(final Object object) throws HBqlException, ResultMissingColumnException;

    GenericValue getOptimizedValue() throws HBqlException;

    Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                final boolean allowsCollections) throws HBqlException;

    boolean isAConstant();

    String asString();

    void reset();
}
