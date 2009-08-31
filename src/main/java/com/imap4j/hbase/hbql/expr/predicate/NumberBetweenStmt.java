package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.NumberLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberBetweenStmt implements PredicateExpr {

    private NumberValue expr = null;
    private final boolean not;
    private NumberValue lower = null, upper = null;

    public NumberBetweenStmt(final NumberValue expr, final boolean not, final NumberValue lower, final NumberValue upper) {
        this.expr = expr;
        this.not = not;
        this.lower = lower;
        this.upper = upper;
    }

    private NumberValue getExpr() {
        return this.expr;
    }

    private NumberValue getLower() {
        return this.lower;
    }

    private NumberValue getUpper() {
        return this.upper;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {
        boolean retval = true;

        if (this.getExpr().optimizeForConstants(context))
            this.expr = new NumberLiteral(this.getExpr().getValue(context));
        else
            retval = false;

        if (this.getLower().optimizeForConstants(context))
            this.lower = new NumberLiteral(this.getLower().getValue(context));
        else
            retval = false;

        if (this.getUpper().optimizeForConstants(context))
            this.upper = new NumberLiteral(this.getUpper().getValue(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public boolean evaluate(final EvalContext context) throws HPersistException {

        final int val = this.getExpr().getValue(context).intValue();
        final boolean retval = val >= this.getLower().getValue(context).intValue()
                               && val <= this.getUpper().getValue(context).intValue();

        return (this.not) ? !retval : retval;
    }

}