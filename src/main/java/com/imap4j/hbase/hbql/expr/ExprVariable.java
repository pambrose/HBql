package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.schema.FieldType;
import com.imap4j.hbase.hbql.schema.VariableAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 1, 2009
 * Time: 8:11:19 PM
 */
public class ExprVariable {

    private final String attribName;
    private final FieldType fieldType;

    public ExprVariable(final String attribName, FieldType fieldType) {
        this.fieldType = fieldType;
        this.attribName = attribName;
    }

    public String getName() {
        return attribName;
    }

    @Override
    public boolean equals(final Object o) {
        return (o instanceof ExprVariable) && this.getName().equals(((ExprVariable)o).getName());
    }

    public VariableAttrib getVariableAttrib(final EvalContext context) {
        return context.getExprSchema().getVariableAttribByVariableName(this.getName());
    }

}
