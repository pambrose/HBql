package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class AttribRef implements Evaluatable {

    private final String field;

    public AttribRef(final String field) {
        this.field = field;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final Object recordObj) {
        return false;
    }
}