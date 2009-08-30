package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NullLiteral implements StringValue {

    public NullLiteral() {
    }

    @Override
    public String getValue(final EvalContext context) {
        return null;
    }
}