package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringExpr implements Evaluatable {

    private final Evaluatable expr;

    public StringExpr(final Evaluatable expr) {
        this.expr = expr;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final Object recordObj) {
        return this.expr.evaluate(classSchema, recordObj);
    }
}