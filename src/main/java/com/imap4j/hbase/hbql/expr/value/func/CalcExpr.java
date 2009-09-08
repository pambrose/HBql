package com.imap4j.hbase.hbql.expr.value.func;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:29:44 PM
 */
public class CalcExpr {

    public enum OP {
        PLUS,
        MINUS,
        MULT,
        DIV,
        MOD,
        NEGATIVE,
        NONE
    }

    private final CalcExpr.OP op;

    public CalcExpr(final OP op) {
        this.op = op;
    }

    protected OP getOp() {
        return op;
    }
}
