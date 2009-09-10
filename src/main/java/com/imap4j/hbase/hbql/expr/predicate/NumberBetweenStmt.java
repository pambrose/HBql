package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbase.HPersistException;
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
    public boolean optimizeForConstants(final Object object) throws HPersistException {
        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.expr = new NumberLiteral(this.getExpr().getValue(object));
        else
            retval = false;

        if (this.getLower().optimizeForConstants(object))
            this.lower = new NumberLiteral(this.getLower().getValue(object));
        else
            retval = false;

        if (this.getUpper().optimizeForConstants(object))
            this.upper = new NumberLiteral(this.getUpper().getValue(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean evaluate(final Object object) throws HPersistException {

        final long numval = this.getExpr().getValue(object).longValue();
        final boolean retval = numval >= this.getLower().getValue(object).longValue()
                               && numval <= this.getUpper().getValue(object).longValue();

        return (this.isNot()) ? !retval : retval;
    }

}