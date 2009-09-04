package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.IntegerValue;
import com.imap4j.hbase.hbql.expr.value.literal.IntegerLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class CalcExpr implements IntegerValue {

    public enum OP {
        PLUS,
        MINUS,
        MULT,
        DIV,
        MOD,
        NEGATIVE,
        NONE
    }

    private IntegerValue expr1 = null, expr2 = null;
    private final OP op;

    public CalcExpr(final IntegerValue expr1) {
        this(expr1, OP.NONE, null);
    }

    public CalcExpr(final IntegerValue expr1, final OP op, final IntegerValue expr2) {
        this.expr1 = expr1;
        this.op = op;
        this.expr2 = expr2;
    }

    private OP getOp() {
        return this.op;
    }

    private IntegerValue getExpr1() {
        return this.expr1;
    }

    private IntegerValue getExpr2() {
        return this.expr2;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr1().getExprVariables();
        retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getExpr1().optimizeForConstants(context))
            this.expr1 = new IntegerLiteral(this.getExpr1().getValue(context));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(context))
            this.expr2 = new IntegerLiteral(this.getExpr2().getValue(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public Number getValue(final EvalContext context) throws HPersistException {

        final int val1 = this.getExpr1().getValue(context).intValue();
        final int val2 = (this.getExpr2() != null) ? (this.getExpr2().getValue(context)).intValue() : 0;

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

        throw new HPersistException("Error in CalcExpr.getValue() " + this.getOp());

    }

    @Override
    public boolean isAConstant() {
        return this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

}