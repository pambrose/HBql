package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;
import com.imap4j.hbase.hql.HPersistException;
import com.imap4j.hbase.hql.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:28:42 PM
 */
public class CondPrimary implements Evaluatable {
    private final Evaluatable expr;

    public CondPrimary(final Evaluatable expr) {
        this.expr = expr;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {
        return this.expr.evaluate(classSchema, recordObj);
    }
}
