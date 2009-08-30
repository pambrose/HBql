package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistException;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 26, 2009
 * Time: 10:18:22 AM
 */
public interface DateValue extends Serializable {

    boolean optimizeForConstants(final EvalContext context) throws HPersistException;

    Date getValue(final EvalContext context) throws HPersistException;
}