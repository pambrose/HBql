package com.imap4j.hbase.hql.expr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public abstract class CompareExpr implements Evaluatable {

    public enum Operator {
        EQ,
        GT,
        GTEQ,
        LT,
        LTEQ,
        LTGT

    }

    public AttribRef attrib;
    public CompareExpr.Operator op;

    protected CompareExpr() {
    }

    protected CompareExpr(final AttribRef attrib, final Operator op) {
        this.attrib = attrib;
        this.op = op;
    }

}