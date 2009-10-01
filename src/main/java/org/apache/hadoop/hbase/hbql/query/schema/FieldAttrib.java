package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:27:00 PM
 */
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
              fieldType,
              mapKeysAsColumns,
              getter,
              setter
        );
        this.field = field;
        setAccessible(this.getField());
    }

    @Override
    public String toString() {
        return this.getSimpleObjectQualifiedName() + " " + this.getFamilyQualifiedName();
    }

    @Override
    public String getObjectQualifiedName() {
        return this.getEnclosingClass().getName() + "." + this.getColumnName();
    }

    public String getSimpleObjectQualifiedName() {
        return this.getEnclosingClass().getSimpleName() + "." + this.getColumnName();
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

    @Override
    public boolean isArray() {
        return this.getField().getType().isArray();
    }

    @Override
    public Object getCurrentValue(final Object recordObj) throws HBqlException {
        try {
            return this.getField().get(recordObj);
        }
        catch (IllegalAccessException e) {
            throw new HBqlException("Error getting value of " + this.getObjectQualifiedName());
        }
    }

    @Override
    public void setCurrentValue(final Object newobj, final long timestamp, final Object val) {
        try {
            this.getField().set(newobj, val);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException("Error setting value of " + this.getObjectQualifiedName());
        }
    }

    @Override
    public Object getVersionedValueMap(final Object recordObj) throws HBqlException {
        // Just call current value for version since we have different fields for each
        return this.getCurrentValue(recordObj);
    }

    @Override
    protected void setVersionedValueMap(final Object newobj, final Map<Long, Object> map) {
        // Just call current value for version since we have different fields for each
        this.setCurrentValue(newobj, 0, map);
    }

    protected static void setAccessible(final Field field) {
        // Unlock private vars
        if (!field.isAccessible())
            field.setAccessible(true);
    }
}