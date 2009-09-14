package com.imap4j.hbase.hbql.expr.value.literal;

import com.imap4j.hbase.hbql.expr.node.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringNullLiteral extends GenericLiteral implements StringValue {

    public StringNullLiteral() {
    }

    @Override
    public String getCurrentValue(final Object object) {
        return null;
    }
}