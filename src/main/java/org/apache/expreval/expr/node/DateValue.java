package org.apache.expreval.expr.node;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public interface DateValue extends GenericValue {

    Long getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}