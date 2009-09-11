package com.imap4j.hbase.hbql.expr.value.literal;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.ExprTreeNode;
import com.imap4j.hbase.hbql.schema.ExprSchema;
import com.imap4j.hbase.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:27:29 PM
 */
public abstract class GenericLiteral implements ExprTreeNode {

    @Override
    public List<ExprVariable> getExprVariables() {
        return Lists.newArrayList();
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {
        return true;
    }

    @Override
    public boolean isAConstant() {
        return true;
    }

    @Override
    public void setSchema(final ExprSchema schema) {
    }
}
