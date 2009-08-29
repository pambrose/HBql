package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.PredicateExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanExpr implements PredicateExpr {

    public enum OP {
        AND,
        OR
    }

    private final PredicateExpr expr1;
    private final PredicateExpr expr2;
    private final BooleanExpr.OP op;

    public BooleanExpr(final PredicateExpr expr1, final BooleanExpr.OP op, final PredicateExpr expr2) {
        this.expr1 = expr1;
        this.expr2 = expr2;
        this.op = op;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {

        if (expr2 == null)
            return expr1.evaluate(context);

        switch (this.op) {
            case OR:
                return expr1.evaluate(context) || expr2.evaluate(context);
            case AND:
                return expr1.evaluate(context) && expr2.evaluate(context);

            default:
                throw new HPersistException("Error in BooleanExpr.evaluate()");

        }
    }
}
