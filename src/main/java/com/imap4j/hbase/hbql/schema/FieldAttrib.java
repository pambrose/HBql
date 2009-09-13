package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:27:00 PM
 */
public abstract class FieldAttrib extends ColumnAttrib {

    private final transient Field field;

    protected FieldAttrib(final Field field,
                          final FieldType fieldType,
                          final String family,
                          final String column,
                          final String getter,
                          final String setter,
                          final boolean mapKeysAsColumns) throws HPersistException {
        super(fieldType,
              family,
              (column != null && column.length() > 0) ? column : field.getName(),
              getter,
              setter,
              mapKeysAsColumns);
        this.field = field;
        setAccessible(this.getField());
    }

    @Override
    public String toString() {
        return this.getObjectQualifiedName();
    }

    public String getVariableName() {
        return this.getField().getName();
    }

    public String getObjectQualifiedName() {
        return this.getEnclosingClass().getName() + "." + this.getVariableName();
    }

    public static String getObjectQualifiedName(final Field field) {
        return field.getDeclaringClass().getName() + "." + field.getName();
    }

    @Override
    public String getEnclosingClassName() {
        return this.getEnclosingClass().getName();
    }

    private Class getEnclosingClass() {
        return this.getField().getDeclaringClass();
    }

    @Override
    protected Method getMethod(final String methodName, final Class<?>... params) throws NoSuchMethodException {
        return this.getEnclosingClass().getDeclaredMethod(methodName, params);
    }

    @Override
    protected Class getComponentType() {
        return this.getField().getType().getComponentType();
    }

    protected Field getField() {
        return this.field;
    }

    public boolean isArray() {
        return this.getField().getType().isArray();
    }

    public Object getValue(final Object recordObj) throws HPersistException {
        try {
            return this.getField().get(recordObj);
        }
        catch (IllegalAccessException e) {
            throw new HPersistException("Error getting value of " + this.getObjectQualifiedName());
        }
    }


    public void setValue(final Object newobj, final Object val) {
        try {
            this.getField().set(newobj, val);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException("Error setting value of " + this.getObjectQualifiedName());
        }
    }
}