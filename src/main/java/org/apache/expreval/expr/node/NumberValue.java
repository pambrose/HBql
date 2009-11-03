package org.apache.expreval.expr.node;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public interface NumberValue extends GenericValue {

    Number getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}
