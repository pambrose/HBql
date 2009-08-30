package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.NumberValue;
import com.imap4j.hbase.hbql.expr.PredicateExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberBetweenStmt implements PredicateExpr {

    private final NumberValue number;
    private final boolean not;
    private final NumberValue lower, upper;

    public NumberBetweenStmt(final NumberValue number, final boolean not, final NumberValue lower, final NumberValue upper) {
        this.number = number;
        this.not = not;
        this.lower = lower;
        this.upper = upper;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {

        final Number objVal = this.number.getValue(context);
        final int val = objVal.intValue();
        final boolean retval = val >= this.getLower().getValue(context).intValue()
                               && val <= this.getUpper().getValue(context).intValue();

        return (this.not) ? !retval : retval;
    }

    private NumberValue getLower() {
        return this.lower;
    }

    private NumberValue getUpper() {
        return this.upper;
    }

}