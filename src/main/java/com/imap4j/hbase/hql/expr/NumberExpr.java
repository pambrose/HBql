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
public class NumberExpr implements Value {

    private final Value expr;

    public NumberExpr(final Value expr) {
        this.expr = expr;
    }

    @Override
    public Object getValue(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {
        return this.expr.getValue(classSchema, recordObj);
    }
}