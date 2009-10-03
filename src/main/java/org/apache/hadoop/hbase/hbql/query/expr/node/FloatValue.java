package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 26, 2009
 * Time: 10:18:22 AM
 */
public interface FloatValue extends NumberValue {

    Float getValue(final Object object) throws HBqlException;
}