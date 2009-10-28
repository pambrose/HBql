package org.apache.expreval.expr.node;

import org.apache.expreval.client.HBqlException;

public interface LongValue extends NumberValue {

    Long getValue(final Object object) throws HBqlException;
}