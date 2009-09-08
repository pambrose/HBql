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
public class NumberBetweenStmt extends GenericBetweenStmt implements PredicateExpr {

    private NumberValue expr = null;
    private NumberValue lower = null, upper = null;

    public NumberBetweenStmt(final NumberValue expr, final boolean not, final NumberValue lower, final NumberValue upper) {
        super(not);
        this.expr = expr;
        this.lower = lower;
        this.upper = upper;
    }

    protected NumberValue getExpr() {
        return this.expr;
    }

    protected NumberValue getLower() {
        return this.lower;
    }

    protected NumberValue getUpper() {
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
    public Boolean evaluate(final EvalContext context) throws HPersistException {

        final int numval = this.getExpr().getValue(context).intValue();
        final boolean retval = numval >= this.getLower().getValue(context).intValue()
                               && numval <= this.getUpper().getValue(context).intValue();

        return (this.isNot()) ? !retval : retval;
    }

}