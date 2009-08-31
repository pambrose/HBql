package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.expr.node.ExprEvalTreeNode;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericBetweenStmt {

    private final boolean not;

    protected GenericBetweenStmt(final boolean not) {
        this.not = not;
    }

    public boolean isNot() {
        return not;
    }

    abstract protected ExprEvalTreeNode getExpr();

    abstract protected ExprEvalTreeNode getLower();

    abstract protected ExprEvalTreeNode getUpper();

    public List<String> getAttribNames() {
        final List<String> retval = this.getExpr().getAttribNames();
        retval.addAll(this.getLower().getAttribNames());
        retval.addAll(this.getUpper().getAttribNames());
        return retval;
    }
}
