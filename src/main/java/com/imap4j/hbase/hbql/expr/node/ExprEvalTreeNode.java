package com.imap4j.hbase.hbql.expr.node;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;

import java.io.Serializable;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 11:52:39 AM
 */
public interface ExprEvalTreeNode extends Serializable {

    boolean optimizeForConstants(final EvalContext context) throws HPersistException;

    List<ExprVariable> getExprVariables();

    boolean isAContant();

}
