package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.DateValue;
import com.imap4j.hbase.hbql.schema.FieldAttrib;

import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateAttribRef implements DateValue {

    private final String attribName;

    public DateAttribRef(final String attribName) {
        this.attribName = attribName;
    }

    @Override
    public Date getValue(final AttribContext context) throws HPersistException {
        final FieldAttrib fieldAttrib = context.getClassSchema().getFieldAttribByField(this.attribName);
        return (Date)fieldAttrib.getValue(context.getRecordObj());
    }

}