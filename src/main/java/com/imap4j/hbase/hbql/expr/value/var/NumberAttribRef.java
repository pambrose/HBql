package com.imap4j.hbase.hbql.expr.value.var;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.schema.FieldAttrib;
import com.imap4j.hbase.hbql.schema.FieldType;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberAttribRef extends GenericAttribRef implements NumberValue {

    public NumberAttribRef(final String attribName) {
        super(FieldType.IntegerType, attribName);
    }

    @Override
    public Number getValue(final EvalContext context) throws HPersistException {
        final FieldAttrib fieldAttrib = this.getExprVar().getFieldAttrib(context);
        return (Number)fieldAttrib.getValue(context.getRecordObj());
    }

}