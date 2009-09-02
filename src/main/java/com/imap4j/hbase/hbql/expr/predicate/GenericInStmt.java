package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.expr.node.ExprEvalTreeNode;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericInStmt extends GenericNotStmt {

    protected GenericInStmt(final boolean not) {
        super(not);
    }

    abstract protected ExprEvalTreeNode getExpr();

}