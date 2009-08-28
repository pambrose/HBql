package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;
import com.imap4j.hbase.hql.HPersistException;
import com.imap4j.hbase.hql.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class WhereExpr implements Predicate {

    private final Predicate expr;

    private long start, end;

    public WhereExpr(final Predicate expr) {
        this.expr = expr;

    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {

        this.start = System.nanoTime();

        final boolean retval = this.expr == null || this.expr.evaluate(classSchema, recordObj);

        this.end = System.nanoTime();

        return retval;
    }

    public long getElapsedNanos() {
        return this.end - this.start;
    }
}