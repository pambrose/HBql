package org.apache.hadoop.hbase.hbql.stmt.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;

import java.util.Map;

public interface MapValue extends GenericValue {

    Map getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}