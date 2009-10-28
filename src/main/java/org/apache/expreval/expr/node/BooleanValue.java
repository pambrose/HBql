package org.apache.expreval.expr.node;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

public interface BooleanValue extends GenericValue {

    Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}