package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.PredicateExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class WhereExpr implements PredicateExpr {

    private final PredicateExpr expr;

    private long start, end;

    public WhereExpr(final PredicateExpr expr) {
        this.expr = expr;

    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {

        this.start = System.nanoTime();

        final boolean retval = (this.expr == null) || (this.expr.evaluate(context));

        this.end = System.nanoTime();

        return retval;
    }

    public long getElapsedNanos() {
        return this.end - this.start;
    }
}