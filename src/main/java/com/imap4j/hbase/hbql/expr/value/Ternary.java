package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.NumberValue;
import com.imap4j.hbase.hbql.expr.PredicateExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class Ternary implements NumberValue {

    private final PredicateExpr pred;
    private final NumberValue expr1, expr2;

    public Ternary(final PredicateExpr pred, final NumberValue expr1, final NumberValue expr2) {
        this.pred = pred;
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    @Override
    public Number getValue(final EvalContext context) throws HPersistException {

        if (this.pred.evaluate(context))
            return this.expr1.getValue(context);
        else
            return this.expr2.getValue(context);
    }
}
