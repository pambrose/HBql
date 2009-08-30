package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.NumberValue;
import com.imap4j.hbase.hbql.expr.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class Substring implements StringValue {

    private final StringValue strExpr;
    private final NumberValue beginNumber, endNumber;

    public Substring(final StringValue strExpr, final NumberValue beginNumber, final NumberValue endNumber) {
        this.strExpr = strExpr;
        this.beginNumber = beginNumber;
        this.endNumber = endNumber;
    }

    @Override
    public String getValue(final EvalContext context) throws HPersistException {

        final String val = this.strExpr.getValue(context);
        final int begin = (Integer)this.beginNumber.getValue(context);
        final int end = (Integer)this.endNumber.getValue(context);
        return val.substring(begin, end);
    }
}