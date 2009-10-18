package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;

public interface ByteValue extends GenericValue {

    Byte getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}