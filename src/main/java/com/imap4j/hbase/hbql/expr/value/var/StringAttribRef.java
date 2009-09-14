package com.imap4j.hbase.hbql.expr.value.var;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.schema.FieldType;
import com.imap4j.hbase.hbql.schema.VariableAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringAttribRef extends GenericAttribRef implements StringValue {

    public StringAttribRef(final String attribName) {
        super(attribName, FieldType.StringType);
    }

    @Override
    public String getCurrentValue(final Object object) throws HPersistException {
        final VariableAttrib variableAttrib = this.getExprVar().getVariableAttrib(this.getSchema());
        return (String)variableAttrib.getCurrentValue(object);
    }
}