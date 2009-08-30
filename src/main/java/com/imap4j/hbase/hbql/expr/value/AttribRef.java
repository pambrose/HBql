package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.ExprType;
import com.imap4j.hbase.hbql.expr.ValueExpr;
import com.imap4j.hbase.hbql.schema.FieldAttrib;

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
    public Object getValue(final AttribContext context) throws HPersistException {

        final FieldAttrib fieldAttrib = context.getClassSchema().getFieldAttribByField(this.attribName);

        switch (this.type) {

            case IntegerType:
            case NumberType: {
                return fieldAttrib.getValue(context.getRecordObj());
            }

            default:
                throw new HPersistException("Unknown type in AttribRef.getValue() - " + type);

        }

    }

}