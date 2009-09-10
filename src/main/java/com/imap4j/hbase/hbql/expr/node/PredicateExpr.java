package com.imap4j.hbase.hbql.expr.node;

import com.imap4j.hbase.hbase.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:38:28 PM
 */
public interface PredicateExpr extends ExprEvalTreeNode {

    Boolean evaluate(final Object object) throws HPersistException;

}
