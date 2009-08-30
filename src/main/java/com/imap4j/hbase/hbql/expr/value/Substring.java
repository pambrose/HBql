package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.StringValue;
import com.imap4j.hbase.hbql.expr.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class Substring implements StringValue {

    private final StringValue strExpr;
    private final ValueExpr beginExpr, endExpr;

    public Substring(final StringValue strExpr, final ValueExpr beginExpr, final ValueExpr endExpr) {
        this.strExpr = strExpr;
        this.beginExpr = beginExpr;
        this.endExpr = endExpr;
    }

    @Override
    public String getValue(final AttribContext context) throws HPersistException {

        final String val = this.strExpr.getValue(context);
        final int begin = (Integer)this.beginExpr.getValue(context);
        final int end = (Integer)this.endExpr.getValue(context);
        return val.substring(begin, end);
    }
}