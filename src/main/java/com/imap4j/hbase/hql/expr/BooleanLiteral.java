package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;
import com.imap4j.hbase.hql.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanLiteral implements Value {

    private final Boolean value;

    public BooleanLiteral(final String text) {
        this.value = text.equalsIgnoreCase("true");
    }

    @Override
    public Object getValue(final ClassSchema classSchema, final HPersistable recordObj) {
        return this.value;
    }
}