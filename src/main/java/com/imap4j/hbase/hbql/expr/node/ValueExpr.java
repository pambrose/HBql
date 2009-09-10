package com.imap4j.hbase.hbql.expr.node;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 3, 2009
 * Time: 8:13:01 PM
 */
public interface ValueExpr extends ExprEvalTreeNode {

    Object getValue(final EvalContext context) throws HPersistException;

}
