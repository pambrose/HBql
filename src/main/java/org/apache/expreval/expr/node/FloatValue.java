package org.apache.expreval.expr.node;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public interface FloatValue extends NumberValue {

    Float getValue(final Object object) throws HBqlException;
}