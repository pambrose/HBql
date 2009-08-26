package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;
import com.imap4j.hbase.hql.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringExpr implements StringValue {

    private final StringValue expr;

    public StringExpr(final StringValue expr) {
        this.expr = expr;
    }

    @Override
    public String getValue(final ClassSchema classSchema, final HPersistable recordObj) {
        return this.expr.getValue(classSchema, recordObj);
    }
}