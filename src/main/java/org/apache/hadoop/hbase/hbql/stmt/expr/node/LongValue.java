package org.apache.hadoop.hbase.hbql.stmt.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public interface LongValue extends NumberValue {

    Long getValue(final Object object) throws HBqlException;
}