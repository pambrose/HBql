package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.NumberValue;

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

    private final NumberValue expr1, expr2;
    private final OP op;

    public CalcExpr(final NumberValue expr1) {
        this(expr1, OP.NONE, null);
    }

    public CalcExpr(final NumberValue expr1, final OP op, final NumberValue expr2) {
        this.expr1 = expr1;
        this.op = op;
        this.expr2 = expr2;
    }

    @Override
    public Number getValue(final AttribContext context) throws HPersistException {

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