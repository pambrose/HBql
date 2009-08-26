package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class SimpleCondExpr implements Evaluatable {

    public Evaluatable expr;

    @Override
    public boolean evaluate(final ClassSchema classSchema, final Object recordObj) {
        return expr.evaluate(nil, nil);
    }
}