package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringLiteral implements StringValue {

    private final String value;

    public StringLiteral(final String value) {
        this.value = value;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {
        return false;
    }

    @Override
    public String getValue(final EvalContext context) {
        return this.value;
    }
}