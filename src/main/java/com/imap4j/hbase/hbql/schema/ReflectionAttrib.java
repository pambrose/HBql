package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HPersistException;

import java.lang.reflect.Field;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:19:35 PM
 */
public class ReflectionAttrib extends VariableAttrib {

    private final Field field;

    public ReflectionAttrib(final Field field) {
        super(FieldType.getFieldType(field.getType()));
        this.field = field;

        setAccessible(this.getField());
    }

    @Override
    public String getVariableName() {
        return this.getField().getName();
    }

    private Field getField() {
        return this.field;
    }

    @Override
    public Object getValue(final Object recordObj) throws HPersistException {
        return null;
    }

}