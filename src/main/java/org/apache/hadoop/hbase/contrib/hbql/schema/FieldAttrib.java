package org.apache.hadoop.hbase.contrib.hbql.schema;

import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public abstract class FieldAttrib extends ColumnAttrib {

    private final transient Field field;

    protected FieldAttrib(final String familyName,
                          final String columnName,
                          final Field field,
                          final FieldType fieldType,
                          final boolean familyDefault,
                          final String getter,
                          final String setter) throws HBqlException {
        super(familyName,
              (columnName != null && columnName.length() > 0) ? columnName : field.getName(),
              field.getName(),
              familyDefault,
              fieldType,
              field.getType().isArray(),
              getter,
              setter,
              null);
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

    public Object getCurrentValue(final Object obj) throws HBqlException {
        try {
            return this.getField().get(obj);
        }
        catch (IllegalAccessException e) {
            throw new HBqlException("Error getting value of " + this.getObjectQualifiedName());
        }
    }

    public void setCurrentValue(final Object obj, final long timestamp, final Object val) {
        try {
            this.getField().set(obj, val);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException("Error setting value of " + this.getObjectQualifiedName());
        }
    }

    public void setFamilyDefaultCurrentValue(final Object obj,
                                             final String name,
                                             final byte[] val) throws HBqlException {

        if (!this.isFamilyDefaultAttrib())
            throw new HBqlException(this.getFamilyQualifiedName() + " not marked as familyDefault");

        Map<String, byte[]> mapVal = (Map<String, byte[]>)this.getCurrentValue(obj);

        if (mapVal == null) {
            mapVal = Maps.newHashMap();
            this.setCurrentValue(obj, 0, mapVal);
        }

        mapVal.put(name, val);
    }

    public void setFamilyDefaultVersionMap(final Object obj,
                                           final String name,
                                           final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException {

        if (!this.isFamilyDefaultAttrib())
            throw new HBqlException(this.getFamilyQualifiedName() + " not marked as familyDefault");

        Map<String, NavigableMap<Long, byte[]>> mapVal = (Map<String, NavigableMap<Long, byte[]>>)this.getCurrentValue(obj);

        if (mapVal == null) {
            mapVal = Maps.newHashMap();
            this.setCurrentValue(obj, 0, mapVal);
        }

        mapVal.put(name, timeStampMap);
    }

    public Map<Long, Object> getVersionMap(final Object obj) throws HBqlException {

        if (!this.isAVersionValue())
            throw new HBqlException(this.getFamilyQualifiedName() + " not marked with @ColumnVersionMap");

        // Just call current value for version since we have different fields for current value and versions
        Map<Long, Object> mapVal = (Map<Long, Object>)this.getCurrentValue(obj);
        if (mapVal == null) {
            mapVal = new TreeMap<Long, Object>();
            this.setCurrentValue(obj, 0, mapVal);
        }
        return mapVal;
    }

    protected static void setAccessible(final Field field) {
        // Unlock private vars
        if (!field.isAccessible())
            field.setAccessible(true);
    }
}