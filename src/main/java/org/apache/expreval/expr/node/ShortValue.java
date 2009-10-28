package org.apache.expreval.expr.node;

import org.apache.expreval.client.HBqlException;

public interface ShortValue extends NumberValue {

    Short getValue(final Object object) throws HBqlException;
}