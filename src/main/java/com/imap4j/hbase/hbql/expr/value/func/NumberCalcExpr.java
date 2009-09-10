package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.expr.value.literal.NumberLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberCalcExpr extends CalcExpr implements NumberValue {

    private NumberValue expr1 = null, expr2 = null;

    public NumberCalcExpr(final NumberValue expr1) {
        this(expr1, CalcExpr.OP.NONE, null);
    }

    public NumberCalcExpr(final NumberValue expr1, final CalcExpr.OP op, final NumberValue expr2) {
        super(op);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    private NumberValue getExpr1() {
        return this.expr1;
    }

    private NumberValue getExpr2() {
        return this.expr2;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr1().getExprVariables();
        if (this.getExpr2() != null)
            retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getExpr1().optimizeForConstants(context))
            this.expr1 = new NumberLiteral(this.getExpr1().getValue(context));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(context))
            this.expr2 = new NumberLiteral(this.getExpr2().getValue(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public Long getValue(final EvalContext context) throws HPersistException {

        final long val1 = this.getExpr1().getValue(context).longValue();
        final long val2 = (this.getExpr2() != null) ? (this.getExpr2().getValue(context)).longValue() : 0;

        switch (this.getOp()) {
            case PLUS:
                return val1 + val2;
            case MINUS:
                return val1 - val2;
            case MULT:
                return val1 * val2;
            case DIV:
                return val1 / val2;
            case MOD:
                return val1 % val2;
            case NEGATIVE:
                return val1 * -1;
            case NONE:
                return val1;
        }

        throw new HPersistException("Error in NumberCalcExpr.getValue() " + this.getOp());

    }

    @Override
    public boolean isAConstant() {
        return this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

}