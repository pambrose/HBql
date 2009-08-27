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
public class AttribRef implements ValueExpr {

    private final String attribName;
    private final ExprType type;

    public AttribRef(final ExprType type, final String attribName) {
        this.type = type;
        this.attribName = attribName;
    }

    @Override
    public Object getValue(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {
        return (String)classSchema.getFieldAttribByField(this.attribName).getValue(recordObj);
    }

}