package org.apache.expreval.expr.node;

import org.apache.expreval.client.HBqlException;

public interface DoubleValue extends NumberValue {

    Double getValue(final Object object) throws HBqlException;
}