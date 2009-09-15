package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.schema.ExprSchema;
import com.imap4j.hbase.hbql.schema.FieldType;
import com.imap4j.hbase.hbql.schema.VariableAttrib;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 1, 2009
 * Time: 8:11:19 PM
 */
public class ExprVariable implements Serializable {

    private final String attribName;
    private final FieldType fieldType;

    public ExprVariable(final String attribName, FieldType fieldType) {
        this.attribName = attribName;
        this.fieldType = fieldType;
    }

    public String getName() {
        return attribName;
    }

    @Override
    public boolean equals(final Object o) {
        return (o instanceof ExprVariable) && this.getName().equals(((ExprVariable)o).getName());
    }

    public VariableAttrib getVariableAttrib(final ExprSchema schema) {
        return schema.getVariableAttribByVariableName(this.getName());
    }

}
