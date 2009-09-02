package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.DateValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.DateLiteral;

import java.util.Date;

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
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {
        boolean retval = true;

        if (this.getExpr().optimizeForConstants(context))
            this.expr = new DateLiteral(this.getExpr().getValue(context));
        else
            retval = false;

        if (this.getLower().optimizeForConstants(context))
            this.lower = new DateLiteral(this.getLower().getValue(context));
        else
            retval = false;

        if (this.getUpper().optimizeForConstants(context))
            this.upper = new DateLiteral(this.getUpper().getValue(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public boolean evaluate(final EvalContext context) throws HPersistException {

        final Date dateval = this.getExpr().getValue(context);
        final boolean retval = dateval.compareTo(this.getLower().getValue(context)) >= 0
                               && dateval.compareTo(this.getUpper().getValue(context)) <= 0;

        return (this.isNot()) ? !retval : retval;
    }
}