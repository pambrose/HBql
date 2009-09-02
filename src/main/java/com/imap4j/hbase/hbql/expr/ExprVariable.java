package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.schema.FieldType;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 1, 2009
 * Time: 8:11:19 PM
 */
public class ExprVariable {

    private final String attribName;
    private final FieldType type;

    public ExprVariable(FieldType type, final String attribName) {
        this.type = type;
        this.attribName = attribName;
    }

    public String getAttribName() {
        return attribName;
    }

    @Override
    public boolean equals(final Object o) {
        return (o instanceof ExprVariable) && this.getAttribName().equals(((ExprVariable)o).getAttribName());
    }

}
