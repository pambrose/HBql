package org.apache.expreval.expr.node;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public interface ShortValue extends NumberValue {

    Short getValue(final Object object) throws HBqlException;
}