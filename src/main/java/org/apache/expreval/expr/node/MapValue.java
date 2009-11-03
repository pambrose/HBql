package org.apache.expreval.expr.node;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.util.Map;

public interface MapValue extends GenericValue {

    Map getValue(final Object object) throws HBqlException, ResultMissingColumnException;
}