package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:28:06 PM
 */
public class CondFactor implements PredicateExpr {

    private final boolean not;
    private final PredicateExpr primary;

    public CondFactor(final boolean not, final PredicateExpr primary) {
        this.not = not;
        this.primary = primary;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {
        if (this.not)
            return !this.primary.evaluate(context);
        else
            return this.primary.evaluate(context);
    }
}
