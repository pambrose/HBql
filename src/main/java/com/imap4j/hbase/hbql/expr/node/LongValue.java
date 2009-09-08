package com.imap4j.hbase.hbql.expr.node;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 8, 2009
 * Time: 10:12:28 AM
 */
public interface LongValue extends ValueExpr {

    Long getValue(final EvalContext context) throws HPersistException;
}
