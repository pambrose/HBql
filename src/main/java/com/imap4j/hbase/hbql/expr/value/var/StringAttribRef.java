package com.imap4j.hbase.hbql.expr.value.var;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.schema.FieldAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringAttribRef extends GenericAttribRef implements StringValue {

    public StringAttribRef(final String attribName) {
        super(attribName);
    }

    @Override
    public String getValue(final EvalContext context) throws HPersistException {
        final FieldAttrib fieldAttrib = context.getClassSchema().getFieldAttribByField(this.getAttribName());
        return (String)fieldAttrib.getValue(context.getRecordObj());
    }
}