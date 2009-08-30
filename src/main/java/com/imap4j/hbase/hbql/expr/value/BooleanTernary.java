package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.BooleanValue;
import com.imap4j.hbase.hbql.expr.PredicateExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class BooleanTernary implements BooleanValue {

    private final PredicateExpr pred;
    private final BooleanValue expr1, expr2;

    public BooleanTernary(final PredicateExpr pred, final BooleanValue expr1, final BooleanValue expr2) {
        this.pred = pred;
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    @Override
    public Boolean getValue(final AttribContext context) throws HPersistException {

        if (this.pred.evaluate(context))
            return this.expr1.getValue(context);
        else
            return this.expr2.getValue(context);
    }
}