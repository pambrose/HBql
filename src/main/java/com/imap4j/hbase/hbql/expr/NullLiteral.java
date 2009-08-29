package com.imap4j.hbase.hbql.expr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NullLiteral implements ValueExpr {

    public NullLiteral() {
    }

    @Override
    public Object getValue(final AttribContext context) {
        return null;
    }
}