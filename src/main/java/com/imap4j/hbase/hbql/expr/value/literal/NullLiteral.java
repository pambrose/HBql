package com.imap4j.hbase.hbql.expr.value.literal;

import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NullLiteral extends GenericLiteral implements StringValue {

    public NullLiteral() {
    }

    @Override
    public String getValue(final EvalContext context) {
        return null;
    }

    @Override
    public boolean isContant() {
        return true;
    }
}