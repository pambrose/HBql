package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringBetweenStmt extends GenericBetweenStmt implements PredicateExpr {

    private StringValue expr = null;
    private StringValue lower = null, upper = null;

    public StringBetweenStmt(final StringValue expr, final boolean not, final StringValue lower, final StringValue upper) {
        super(not);
        this.expr = expr;
        this.lower = lower;
        this.upper = upper;
    }

    protected StringValue getExpr() {
        return this.expr;
    }

    protected StringValue getLower() {
        return this.lower;
    }

    protected StringValue getUpper() {
        return this.upper;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {
        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.expr = new StringLiteral(this.getExpr().getValue(object));
        else
            retval = false;

        if (this.getLower().optimizeForConstants(object))
            this.lower = new StringLiteral(this.getLower().getValue(object));
        else
            retval = false;

        if (this.getUpper().optimizeForConstants(object))
            this.upper = new StringLiteral(this.getUpper().getValue(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean evaluate(final Object object) throws HPersistException {

        final String strval = this.getExpr().getValue(object);
        final boolean retval = strval.compareTo(this.getLower().getValue(object)) >= 0
                               && strval.compareTo(this.getUpper().getValue(object)) <= 0;

        return (this.isNot()) ? !retval : retval;
    }
}