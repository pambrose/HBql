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
public class AttribRef implements AttribValue {

    private final String attribName;
    private final Class clazz;

    public AttribRef(final Class clazz, final String attribName) {
        this.clazz = clazz;
        this.attribName = attribName;
    }

    @Override
    public Object getValue(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {
        return (String)classSchema.getFieldAttribByField(this.attribName).getValue(recordObj);
    }

}