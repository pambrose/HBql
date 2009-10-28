package org.apache.expreval.expr.node;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;

import java.util.Map;

public interface MapValue extends GenericValue {

    Map getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}