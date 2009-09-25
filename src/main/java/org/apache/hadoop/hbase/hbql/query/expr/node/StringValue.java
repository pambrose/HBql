package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 7:16:27 PM
 */
public interface StringValue extends ValueExpr {

    String getValue(final Object object) throws HBqlException;
}
