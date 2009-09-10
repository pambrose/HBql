package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.node.DateValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.DateLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateBetweenStmt extends GenericBetweenStmt implements PredicateExpr {

    private DateValue expr = null;
    private DateValue lower = null, upper = null;

    public DateBetweenStmt(final DateValue expr, final boolean not, final DateValue lower, final DateValue upper) {
        super(not);
        this.expr = expr;
        this.lower = lower;
        this.upper = upper;
    }

    protected DateValue getExpr() {
        return this.expr;
    }

    protected DateValue getLower() {
        return this.lower;
    }

    protected DateValue getUpper() {
        return this.upper;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {
        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.expr = new DateLiteral(this.getExpr().getValue(object));
        else
            retval = false;

        if (this.getLower().optimizeForConstants(object))
            this.lower = new DateLiteral(this.getLower().getValue(object));
        else
            retval = false;

        if (this.getUpper().optimizeForConstants(object))
            this.upper = new DateLiteral(this.getUpper().getValue(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean evaluate(final Object object) throws HPersistException {

        final long dateval = this.getExpr().getValue(object);
        final boolean retval = dateval >= this.getLower().getValue(object)
                               && dateval <= this.getUpper().getValue(object);

        return (this.isNot()) ? !retval : retval;
    }
}