package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;

public interface DateValue extends GenericValue {

    Long getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}