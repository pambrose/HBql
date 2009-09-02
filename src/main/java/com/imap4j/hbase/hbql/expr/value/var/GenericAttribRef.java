package com.imap4j.hbase.hbql.expr.value.var;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.ExprEvalTreeNode;
import com.imap4j.hbase.hbql.schema.FieldType;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:30:57 PM
 */
public abstract class GenericAttribRef implements ExprEvalTreeNode {

    private final ExprVariable exprVar;

    protected GenericAttribRef(final FieldType type, final String attribName) {
        this.exprVar = new ExprVariable(type, attribName);
    }

    public ExprVariable getExprVar() {
        return this.exprVar;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        return Lists.newArrayList(this.getExprVar());
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {
        return false;
    }

    @Override
    public boolean isContant() {
        return false;
    }
}
