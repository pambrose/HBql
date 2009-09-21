package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 3, 2009
 * Time: 8:13:01 PM
 */
public interface ValueExpr extends ExprTreeNode {

    Object getValue(final Object object) throws HPersistException;

}
