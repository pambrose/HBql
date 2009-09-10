package com.imap4j.hbase.hbql.expr.value.var;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.schema.FieldType;
import com.imap4j.hbase.hbql.schema.VariableAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class LongAttribRef extends GenericAttribRef implements NumberValue {

    public LongAttribRef(final String attribName) {
        super(attribName, FieldType.LongType);
    }

    @Override
    public Long getValue(final Object object) throws HPersistException {
        final VariableAttrib variableAttrib = this.getExprVar().getVariableAttrib(this.getSchema());
        return (Long)variableAttrib.getValue(object);
    }

}