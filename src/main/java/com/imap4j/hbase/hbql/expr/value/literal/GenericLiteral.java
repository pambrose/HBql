package com.imap4j.hbase.hbql.expr.value.literal;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.ExprEvalTreeNode;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:27:29 PM
 */
public abstract class GenericLiteral implements ExprEvalTreeNode {

    @Override
    public List<ExprVariable> getExprVariables() {
        return Lists.newArrayList();
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {
        return false;
    }

    @Override
    public boolean isAConstant() {
        return true;
    }
}
