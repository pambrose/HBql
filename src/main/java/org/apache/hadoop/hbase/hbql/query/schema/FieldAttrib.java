package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

public abstract class FieldAttrib extends ColumnAttrib {

    private final transient Field field;

    protected FieldAttrib(final String familyName,
                          final String columnName,
                          final Field field,
                          final FieldType fieldType,
                          final boolean mapKeysAsColumns,
                          final String getter,
                          final String setter) throws HBqlException {
        super(familyName,
              (columnName != null && columnName.length() > 0) ? columnName : field.getName(),
              field.getName(),
              mapKeysAsColumns,
              fieldType,
              field.getType().isArray(),
              getter,
              setter
        );
        this.field = field;
        setAccessible(this.getField());
    }

    public String toString() {
        return this.getSimpleObjectQualifiedName() + " " + this.getFamilyQualifiedName();
    }

    public String getNameToUseInExceptions() {
        return this.getObjectQualifiedName();
    }

    public String getObjectQualifiedName() {
        return this.getEnclosingClassName() + "." + this.getField().getName();
    }

    public String getSimpleObjectQualifiedName() {
        return this.getEnclosingClass().getSimpleName() + "." + this.getField().getName();
    }

    public static String getObjectQualifiedName(final Field field) {
        return field.getDeclaringClass().getName() + "." + field.getName();
    }

    public String getEnclosingClassName() {
        return this.getEnclosingClass().getName();
    }

    private Class getEnclosingClass() {
        return this.getField().getDeclaringClass();
    }

    protected Method getMethod(final String methodName, final Class<?>... params) throws NoSuchMethodException {
        return this.getEnclosingClass().getDeclaredMethod(methodName, params);
    }

    protected Class getComponentType() {
        return this.getField().getType().getComponentType();
    }

    protected Field getField() {
        return this.field;
    }

    public Object getCurrentValue(final Object recordObj) throws HBqlException {
        try {
            return this.getField().get(recordObj);
        }
        catch (IllegalAccessException e) {
            throw new HBqlException("Error getting value of " + this.getObjectQualifiedName());
        }
    }

    public void setCurrentValue(final Object newobj, final long timestamp, final Object val) {
        try {
            this.getField().set(newobj, val);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException("Error setting value of " + this.getObjectQualifiedName());
        }
    }

    public Map<Long, Object> getVersionValueMapValue(final Object recordObj) throws HBqlException {
        // Just call current value for version since we have different fields for each
        return (Map<Long, Object>)this.getCurrentValue(recordObj);
    }

    public void setVersionValueMapValue(final Object newobj, final Map<Long, Object> map) {
        // Just call current value for version since we have different fields for each
        this.setCurrentValue(newobj, 0, map);
    }

    protected static void setAccessible(final Field field) {
        // Unlock private vars
        if (!field.isAccessible())
            field.setAccessible(true);
    }
}