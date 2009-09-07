package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:19:35 PM
 */
public class VarDescAttrib extends VariableAttrib {

    private final String name;
    private final FieldType type;

    public VarDescAttrib(final String name, final FieldType type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public String getVariableName() {
        return this.name;
    }

    @Override
    public FieldType getFieldType() {
        return this.type;
    }

    @Override
    public Object getValue(final HPersistable recordObj) throws HPersistException {
        return null;
    }

    @Override
    public String getFamilyName() {
        return null;
    }

    @Override
    public String getFamilyQualifiedName() {
        return null;
    }

    @Override
    public boolean isMapKeysAsColumns() {
        return false;
    }

}
