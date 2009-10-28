package org.apache.expreval.expr.node;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

import java.util.Map;

public interface MapValue extends GenericValue {

    Map getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}