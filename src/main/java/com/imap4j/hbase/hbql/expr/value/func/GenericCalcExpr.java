package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbql.expr.node.ValueExpr;
import com.imap4j.hbase.hbql.expr.value.GenericTwoExprExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:29:44 PM
 */
public class GenericCalcExpr<T extends ValueExpr> extends GenericTwoExprExpr<T> {

    public enum OP {
        PLUS,
        MINUS,
        MULT,
        DIV,
        MOD,
        NEGATIVE,
        NONE
    }

    private final GenericCalcExpr.OP op;

    public GenericCalcExpr(final T expr1, final OP op, final T expr2) {
        super(expr1, expr2);
        this.op = op;
    }

    protected OP getOp() {
        return op;
    }

}
