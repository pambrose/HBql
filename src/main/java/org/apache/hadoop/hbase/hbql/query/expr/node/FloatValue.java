package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public interface FloatValue extends NumberValue {

    Float getValue(final Object object) throws HBqlException;
}