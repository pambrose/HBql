package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:19:35 PM
 */
public class VarDescAttrib extends VariableAttrib {

    private final String variableName;
    private final String typeName;

    public VarDescAttrib(final VarDesc var) {
        super(var.getFieldType());
        this.variableName = var.getVariableName();
        this.typeName = var.getTypeName();
    }

    public String getTypeName() {
        return this.typeName;
    }

    @Override
    public String getVariableName() {
        return this.variableName;
    }

    @Override
    public Object getValue(final Object recordObj) throws HPersistException {
        return null;
    }

}
