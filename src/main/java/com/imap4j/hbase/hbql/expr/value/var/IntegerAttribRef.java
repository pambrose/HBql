package com.imap4j.hbase.hbql.expr.value.var;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.schema.FieldType;
import com.imap4j.hbase.hbql.schema.VariableAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class IntegerAttribRef extends GenericAttribRef implements NumberValue {

    public IntegerAttribRef(final String attribName) {
        super(attribName, FieldType.IntegerType);
    }

    @Override
    public Integer getValue(final EvalContext context) throws HPersistException {
        final VariableAttrib variableAttrib = this.getExprVar().getVariableAttrib(context);
        return (Integer)variableAttrib.getValue(context.getObject());
    }

}