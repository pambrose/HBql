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
public class StringExpr implements AttribValue {

    private final AttribValue expr;

    public StringExpr(final AttribValue expr) {
        this.expr = expr;
        this.expr.
    }

    @Override
    public Object getValue(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {
        return this.expr.getValue(classSchema, recordObj);
    }
}