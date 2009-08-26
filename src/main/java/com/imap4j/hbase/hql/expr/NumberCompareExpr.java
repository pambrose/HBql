package com.imap4j.hbase.hql.expr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class NumberCompareExpr extends CompareExpr {

    public Number value;


    public NumberCompareExpr(final Number value) {
        this.value = value;
    }

    public NumberCompareExpr(final String attribName, final Operator op, final Number value) {
        super(attribName, op);
        this.value = value;
    }
}