package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.ExprEvalTreeNode;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 1:51:03 PM
 */
public abstract class GenericTernary {

    protected PredicateExpr pred = null;

    public GenericTernary(final PredicateExpr pred) {
        this.pred = pred;
    }

    protected PredicateExpr getPred() {
        return this.pred;
    }

    abstract protected ExprEvalTreeNode getExpr1();

    abstract protected ExprEvalTreeNode getExpr2();

    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getPred().getExprVariables();
        retval.addAll(this.getExpr1().getExprVariables());
        retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

}
