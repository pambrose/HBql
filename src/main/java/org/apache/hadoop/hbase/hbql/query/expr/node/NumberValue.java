package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public interface NumberValue extends GenericValue {

    Number getValue(final Object object) throws HBqlException;
}
