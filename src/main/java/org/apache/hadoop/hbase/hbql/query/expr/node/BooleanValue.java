package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 7:16:27 PM
 */
public interface BooleanValue extends ValueExpr {

    Boolean getValue(final Object object) throws HPersistException;
}