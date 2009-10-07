package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public interface DoubleValue extends NumberValue {

    Double getValue(final Object object) throws HBqlException;
}