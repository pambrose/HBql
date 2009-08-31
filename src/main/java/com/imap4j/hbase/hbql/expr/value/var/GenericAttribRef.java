package com.imap4j.hbase.hbql.expr.value.var;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.ExprEvalTreeNode;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:30:57 PM
 */
public abstract class GenericAttribRef implements ExprEvalTreeNode {

    private final String attribName;

    protected GenericAttribRef(final String attribName) {
        this.attribName = attribName;
    }

    protected String getAttribName() {
        return attribName;
    }

    @Override
    public List<String> getAttribNames() {
        return Lists.newArrayList(this.getAttribName());
    }


    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {
        return false;
    }

}
