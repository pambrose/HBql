package com.imap4j.hbase.hql.expr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class StringCompareExpr extends CompareExpr {

    public String strValue;


    public StringCompareExpr(final String strValue) {
        this.strValue = strValue;
    }

    public StringCompareExpr(final String attribName, final Operator op, final String strValue) {
        super(attribName, op);
        this.strValue = strValue;
    }
}
