package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Map;

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

    public abstract Object getCurrentValue(final Object recordObj) throws HPersistException;

    protected abstract void setCurrentValue(final Object newobj, final long timestamp, final Object val);

    public abstract Object getVersionedValueMap(final Object recordObj) throws HPersistException;

    protected abstract void setVersionedValueMap(final Object newobj, final Map<Long, Object> map);

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
