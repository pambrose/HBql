package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.BooleanValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanLiteral implements BooleanValue {

    private final Boolean value;

    public BooleanLiteral(final String text) {
        this.value = text.equalsIgnoreCase("true");
    }

    @Override
    public Boolean getValue(final AttribContext context) {
        return this.value;
    }
}