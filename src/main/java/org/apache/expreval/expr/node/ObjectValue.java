package org.apache.expreval.expr.node;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public interface ObjectValue extends GenericValue {

    Object getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}