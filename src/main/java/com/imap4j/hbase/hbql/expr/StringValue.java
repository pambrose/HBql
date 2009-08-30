package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 7:16:27 PM
 */
public interface StringValue extends ValueExpr {

    @Override
    String getValue(final AttribContext context) throws HPersistException;
}
