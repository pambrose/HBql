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
public class StringAttribRef implements StringValue {

    private final String attribName;

    public StringAttribRef(final String attribName) {
        this.attribName = attribName;
    }

    @Override
    public String getValue(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {
        return (String)classSchema.getFieldAttribByField(this.attribName).getValue(recordObj);
    }
}