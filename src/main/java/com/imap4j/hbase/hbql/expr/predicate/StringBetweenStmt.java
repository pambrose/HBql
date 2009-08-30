package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.PredicateExpr;
import com.imap4j.hbase.hbql.expr.StringValue;
import com.imap4j.hbase.hbql.expr.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringBetweenStmt implements PredicateExpr {

    private final StringValue expr;
    private final boolean not;
    private final StringValue lower, upper;

    public StringBetweenStmt(final StringValue expr, final boolean not, final StringValue lower, final StringValue upper) {
        this.expr = expr;
        this.not = not;
        this.lower = lower;
        this.upper = upper;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {

        final String val = (String)this.expr.getValue(context);
        final boolean retval = val.compareTo((String)this.getLower().getValue(context)) >= 0
                               && val.compareTo((String)this.getUpper().getValue(context)) <= 0;

        return (this.not) ? !retval : retval;
    }

    private ValueExpr getLower() {
        return this.lower;
    }

    private ValueExpr getUpper() {
        return this.upper;
    }

}