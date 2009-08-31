package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.expr.value.literal.NumberLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class CalcExpr implements NumberValue {

    public enum OP {
        PLUS,
        MINUS,
        MULT,
        DIV,
        MOD,
        NEGATIVE,
        NONE
    }

    private NumberValue expr1 = null, expr2 = null;
    private final OP op;

    public CalcExpr(final NumberValue expr1) {
        this(expr1, OP.NONE, null);
    }

    public CalcExpr(final NumberValue expr1, final OP op, final NumberValue expr2) {
        this.expr1 = expr1;
        this.op = op;
        this.expr2 = expr2;
    }

    private NumberValue getExpr1() {
        return this.expr1;
    }

    private NumberValue getExpr2() {
        return this.expr2;
    }

    @Override
    public List<String> getAttribNames() {
        final List<String> retval = this.getExpr1().getAttribNames();
        retval.addAll(this.getExpr2().getAttribNames());
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
    public Number getValue(final EvalContext context) throws HPersistException {

        final int val1 = ((Number)this.expr1.getValue(context)).intValue();
        final int val2 = (this.expr2 != null) ? ((Number)this.expr2.getValue(context)).intValue() : 0;

        switch (this.op) {
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

        throw new HPersistException("Error in CalcExpr.getValue() " + this.op);

    }
}