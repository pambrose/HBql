package com.imap4j.hbase.hbql.expr.node;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 7:16:27 PM
 */
public interface StringValue extends ValueExpr {

    String getValue(final EvalContext context) throws HPersistException;
}
