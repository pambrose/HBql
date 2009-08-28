package com.imap4j.hbase.hql.expr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public abstract class CompareExpr implements PredicateExpr {

    public enum Operator {
        EQ,
        GT,
        GTEQ,
        LT,
        LTEQ,
        NOTEQ
    }

    private final CompareExpr.Operator operator;

    protected CompareExpr(final Operator operator) {
        this.operator = operator;
    }

    protected Operator getOperator() {
        return operator;
    }
}