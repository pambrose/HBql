package org.apache.expreval.expr.node;

import org.apache.expreval.client.HBqlException;

public interface IntegerValue extends NumberValue {

    Integer getValue(final Object object) throws HBqlException;
}