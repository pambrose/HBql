package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:19:35 PM
 */
public class VarDescAttrib extends VariableAttrib {

    private final String name;

    public VarDescAttrib(final String name, final FieldType fieldType) {
        super(fieldType);
        this.name = name;
    }

    @Override
    public String getVariableName() {
        return this.name;
    }

    @Override
    public Object getValue(final Object recordObj) throws HPersistException {
        return null;
    }

}
