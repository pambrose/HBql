package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class OrExpr implements PredicateExpr {

    private final PredicateExpr expr1;
    private final PredicateExpr expr2;

    public OrExpr(final PredicateExpr expr1, final PredicateExpr expr2) {
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {
        if (expr2 == null)
            return expr1.evaluate(context);
        else
            return expr1.evaluate(context) || expr2.evaluate(context);
    }
}
