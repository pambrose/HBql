package com.imap4j.hbase.hbql.expr.predicate;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public abstract class CompareExpr {

    public enum OP {
        EQ,
        GT,
        GTEQ,
        LT,
        LTEQ,
        NOTEQ
    }

    private final OP op;

    protected CompareExpr(final OP op) {
        this.op = op;
    }

    protected OP getOp() {
        return op;
    }
}