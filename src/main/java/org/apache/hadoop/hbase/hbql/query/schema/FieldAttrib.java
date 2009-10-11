package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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

    public void setKeysAsColumnsValue(final Object newobj,
                                      final String mapKey,
                                      final Object val) throws HBqlException {

        if (!this.isMapKeysAsColumnsColumn())
            throw new HBqlException(this.getFamilyQualifiedName() + " not marked as mapKeysAsColumns");

        Map<String, Object> mapVal = (Map<String, Object>)this.getCurrentValue(newobj);

        if (mapVal == null) {
            mapVal = Maps.newHashMap();
            this.setCurrentValue(newobj, 0, mapVal);
        }

        mapVal.put(mapKey, val);
    }

    public Map<Long, Object> getVersionObjectValueMap(final Object newobj) throws HBqlException {

        if (!this.isAVersionValue())
            throw new HBqlException(this.getFamilyQualifiedName() + " not marked with @HColumnVersionMap");

        // Just call current value for version since we have different fields for current value and versions
        Map<Long, Object> mapVal = (Map<Long, Object>)this.getCurrentValue(newobj);
        if (mapVal == null) {
            mapVal = new TreeMap<Long, Object>();
            this.setCurrentValue(newobj, 0, mapVal);
        }
        return mapVal;
    }

    public Map<Long, Object> getKeysAsColumnsVersionMap(final Object newobj, final String mapKey) throws HBqlException {

        if (!this.isAVersionValue())
            throw new HBqlException(this.getFamilyQualifiedName() + " not marked with @HColumnVersionMap");

        // TODO Should make sure that this refers to column marked as mapKeysAsColumns as well

        // Just call current value for version since we have different fields for current value and versions
        Map<String, Map<Long, Object>> mapVal = (Map<String, Map<Long, Object>>)this.getCurrentValue(newobj);
        if (mapVal == null) {
            mapVal = new HashMap<String, Map<Long, Object>>();
            this.setCurrentValue(newobj, 0, mapVal);
        }

        Map<Long, Object> mapForKey = mapVal.get(mapKey);
        if (mapForKey == null) {
            mapForKey = new TreeMap<Long, Object>();
            mapVal.put(mapKey, mapForKey);
        }
        return mapForKey;
    }

    protected static void setAccessible(final Field field) {
        // Unlock private vars
        if (!field.isAccessible())
            field.setAccessible(true);
    }
}