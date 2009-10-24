package org.apache.hadoop.hbase.hbql.stmt.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public interface IntegerValue extends NumberValue {

    Integer getValue(final Object object) throws HBqlException;
}