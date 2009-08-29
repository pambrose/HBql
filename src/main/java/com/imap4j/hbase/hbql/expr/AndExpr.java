package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:23:42 PM
 */
public class AndExpr implements PredicateExpr {

    private final PredicateExpr expr1;
    private final PredicateExpr expr2;

    public AndExpr(final PredicateExpr expr1, final PredicateExpr expr2) {
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {
        if (this.expr2 == null)
            return this.expr1.evaluate(context);
        else
            return this.expr1.evaluate(context) && this.expr2.evaluate(context);
    }
}
