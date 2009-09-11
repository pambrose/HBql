package com.imap4j.hbase.hbql.expr.value.var;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.ExprTreeNode;
import com.imap4j.hbase.hbql.schema.ExprSchema;
import com.imap4j.hbase.hbql.schema.FieldType;
import com.imap4j.hbase.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:30:57 PM
 */
public abstract class GenericAttribRef implements ExprTreeNode {

    private final ExprVariable exprVar;
    private ExprSchema schema = null;

    protected GenericAttribRef(final String attribName, final FieldType fieldType) {
        this.exprVar = new ExprVariable(attribName, fieldType);
    }

    public ExprVariable getExprVar() {
        return this.exprVar;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        return Lists.newArrayList(this.getExprVar());
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {
        return false;
    }

    @Override
    public boolean isAConstant() {
        return false;
    }

    public void setSchema(final ExprSchema schema) {
        this.schema = schema;
    }

    public ExprSchema getSchema() {
        return schema;
    }

}
