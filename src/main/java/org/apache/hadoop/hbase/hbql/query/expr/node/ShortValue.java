package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public interface ShortValue extends NumberValue {

    Short getValue(final Object object) throws HBqlException;
}