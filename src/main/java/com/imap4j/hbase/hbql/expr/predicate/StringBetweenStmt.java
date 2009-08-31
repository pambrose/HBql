package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringBetweenStmt implements PredicateExpr {

    private StringValue expr = null;
    private final boolean not;
    private StringValue lower = null, upper = null;

    public StringBetweenStmt(final StringValue expr, final boolean not, final StringValue lower, final StringValue upper) {
        this.expr = expr;
        this.not = not;
        this.lower = lower;
        this.upper = upper;
    }

    private StringValue getExpr() {
        return this.expr;
    }

    private StringValue getLower() {
        return this.lower;
    }

    private StringValue getUpper() {
        return this.upper;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {
        boolean retval = true;

        if (this.getExpr().optimizeForConstants(context))
            this.expr = new StringLiteral(this.getExpr().getValue(context));
        else
            retval = false;

        if (this.getLower().optimizeForConstants(context))
            this.lower = new StringLiteral(this.getLower().getValue(context));
        else
            retval = false;

        if (this.getUpper().optimizeForConstants(context))
            this.upper = new StringLiteral(this.getUpper().getValue(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public boolean evaluate(final EvalContext context) throws HPersistException {

        final String str = this.getExpr().getValue(context);
        final boolean retval = str.compareTo(this.getLower().getValue(context)) >= 0
                               && str.compareTo(this.getUpper().getValue(context)) <= 0;

        return (this.not) ? !retval : retval;
    }
}