package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public interface BooleanValue extends GenericValue {

    Boolean getValue(final Object object) throws HBqlException;
}