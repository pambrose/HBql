package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HPersistException;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 6:07:31 PM
 */
public abstract class VariableAttrib implements Serializable {

    private final FieldType fieldType;

    protected VariableAttrib(final FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public abstract String getVariableName();

    public abstract Object getValue(final Object recordObj) throws HPersistException;

    public FieldType getFieldType() {
        return this.fieldType;
    }

    public boolean isKey() {
        return false;
    }

    protected static void setAccessible(final Field field) {
        // Unlock private vars
        if (!field.isAccessible())
            field.setAccessible(true);
    }

}
