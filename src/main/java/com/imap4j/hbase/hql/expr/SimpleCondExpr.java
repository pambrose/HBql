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
public class SimpleCondExpr implements Evaluatable {

    private final Evaluatable expr;

    public SimpleCondExpr(final Evaluatable expr) {
        this.expr = expr;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {
        return expr.evaluate(classSchema, recordObj);
    }
}