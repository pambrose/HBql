package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.PredicateExpr;

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
        final boolean retval = this.primary.evaluate(context);
        return (this.not) ? !retval : retval;

    }
}
