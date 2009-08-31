package com.imap4j.hbase.hbql.expr.node;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:38:28 PM
 */
public interface PredicateExpr extends ExprEvalTreeNode {

    boolean evaluate(final EvalContext context) throws HPersistException;

}
