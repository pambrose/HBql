package org.apache.expreval.expr.node;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;

public interface ObjectValue extends GenericValue {

    Object getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}