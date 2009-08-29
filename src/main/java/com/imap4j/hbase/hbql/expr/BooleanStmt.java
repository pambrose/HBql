package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanStmt implements PredicateExpr {

    private final ValueExpr expr;

    public BooleanStmt(final ValueExpr expr) {
        this.expr = expr;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {
        return ((Boolean)this.expr.getValue(context)).booleanValue();
    }
}