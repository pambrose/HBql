package org.apache.hadoop.hbase.hbql.stmt.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;

public interface StringValue extends GenericValue {

    String getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}
