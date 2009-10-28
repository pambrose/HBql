package org.apache.expreval.expr.node;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;

public interface DateValue extends GenericValue {

    Long getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}