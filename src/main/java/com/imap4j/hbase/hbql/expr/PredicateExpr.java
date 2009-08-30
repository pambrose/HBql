package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistException;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:38:28 PM
 */
public interface PredicateExpr extends Serializable {

    boolean optimizeForConstants(final EvalContext context) throws HPersistException;

    boolean evaluate(final EvalContext context) throws HPersistException;

}
