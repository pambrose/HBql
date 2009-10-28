package org.apache.expreval.expr.node;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;

public interface NumberValue extends GenericValue {

    Number getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}
