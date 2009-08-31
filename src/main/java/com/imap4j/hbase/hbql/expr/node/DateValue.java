package com.imap4j.hbase.hbql.expr.node;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;

import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 26, 2009
 * Time: 10:18:22 AM
 */
public interface DateValue extends ExprEvalTreeNode {

    Date getValue(final EvalContext context) throws HPersistException;
}