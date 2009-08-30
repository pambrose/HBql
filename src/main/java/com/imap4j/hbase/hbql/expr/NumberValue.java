package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistException;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 26, 2009
 * Time: 10:18:22 AM
 */
public interface NumberValue extends Serializable {

    boolean optimizeForConstants(final EvalContext context) throws HPersistException;

    Number getValue(final EvalContext context) throws HPersistException;
}
