package com.imap4j.hbase.hbql.expr.value.var;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.DateValue;
import com.imap4j.hbase.hbql.schema.FieldType;
import com.imap4j.hbase.hbql.schema.VariableAttrib;

import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateAttribRef extends GenericAttribRef implements DateValue {

    public DateAttribRef(final String attribName) {
        super(FieldType.DateType, attribName);
    }

    @Override
    public Long getValue(final EvalContext context) throws HPersistException {
        final VariableAttrib variableAttrib = this.getExprVar().getVariableAttrib(context);
        return ((Date)variableAttrib.getValue(context.getObject())).getTime();
    }

}