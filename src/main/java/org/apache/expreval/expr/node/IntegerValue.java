package org.apache.expreval.expr.node;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public interface IntegerValue extends NumberValue {

    Integer getValue(final Object object) throws HBqlException;
}