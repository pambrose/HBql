package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:38:28 PM
 */
public interface PredicateExpr extends ExprTreeNode {

    Boolean evaluate(final Object object) throws HPersistException;

}
