package org.apache.expreval.expr.node;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public interface StringValue extends GenericValue {

    String getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}
