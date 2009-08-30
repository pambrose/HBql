package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.BooleanValue;
import com.imap4j.hbase.hbql.expr.PredicateExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanStmt implements PredicateExpr {

    private final BooleanValue expr;

    public BooleanStmt(final BooleanValue expr) {
        this.expr = expr;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {
        return this.expr.getValue(context).booleanValue();
    }
}