package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.BooleanValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.BooleanLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanPredicate implements BooleanValue {

    private PredicateExpr expr = null;

    public BooleanPredicate(final PredicateExpr expr) {
        this.expr = expr;
    }

    private PredicateExpr getExpr() {
        return expr;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        return this.getExpr().getExprVariables();
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(context))
            this.expr = new BooleanLiteral(this.getExpr().evaluate(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public boolean getValue(final EvalContext context) throws HPersistException {
        return this.getExpr().evaluate(context);
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant();
    }

}