package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.stmt.args.DefaultArg;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;

public abstract class ColumnAttrib implements Serializable {

    protected final String familyName;
    protected final String columnName;
    protected final String aliasName;
    private final FieldType fieldType;
    private byte[] familyBytes = null;
    private byte[] columnBytes = null;
    protected final String getter;
    protected final String setter;
    protected final boolean mapKeysAsColumns;
    protected final boolean familyDefault;
    protected final boolean isArray;
    protected transient Method getterMethod = null;
    protected transient Method setterMethod = null;
    protected final Object defaultValue;

    protected ColumnAttrib(final String familyName,
                           final String columnName,
                           final String aliasName,
                           final boolean mapKeysAsColumns,
                           final boolean familyDefault,
                           final FieldType fieldType,
                           final boolean isArray,
                           final String getter,
                           final String setter,
                           final GenericValue defaultValueExpr) throws HBqlException {

        this.familyName = familyName;
        this.columnName = columnName;
        this.aliasName = aliasName;
        this.mapKeysAsColumns = mapKeysAsColumns;
        this.familyDefault = familyDefault;
        this.fieldType = fieldType;
        this.isArray = isArray;
        this.getter = getter;
        this.setter = setter;
        this.defaultValue = this.evaluateDefaultValue(defaultValueExpr);
    }

    public Object getDefaultValue() {
        return this.defaultValue;
    }

    private Object evaluateDefaultValue(final GenericValue defaultValueExpr) throws HBqlException {

        if (defaultValueExpr == null)
            return null;

        if (this.isKeyAttrib())
            throw new HBqlException("Default values are not valid for key values: " + this.getNameToUseInExceptions());

        if (this.isArray())
            throw new HBqlException("Default values are not valid for array values: " + this.getNameToUseInExceptions());

        // This will apply only to Annotations
        if (this.isAVersionValue() && !this.isACurrentValue())
            throw new HBqlException("Default values are not valid for version values: " + this.getNameToUseInExceptions());

        if (this.isMapKeysAsColumnsAttrib())
            throw new HBqlException("Default values are not valid for MapKeysAsColumns values: " + this.getNameToUseInExceptions());

        final Class<? extends GenericValue> type = this.getFieldType().getExprType();

        if (type == null)
            throw new HBqlException("Default values are not valid for: " + this.getNameToUseInExceptions());

        final DefaultArg defaultArg = new DefaultArg(type, defaultValueExpr);
        return defaultArg.getValue();
    }

    public abstract Object getCurrentValue(final Object obj) throws HBqlException;

    public abstract void setCurrentValue(final Object obj,
                                         final long timestamp,
                                         final Object val) throws HBqlException;

    public abstract Map<Long, Object> getVersionMap(final Object obj) throws HBqlException;

    public abstract void setKeysAsColumnsValue(final Object obj,
                                               final String mapKey,
                                               final Object val) throws HBqlException;

    public abstract Map<Long, Object> getKeysAsColumnsVersionMap(final Object obj,
                                                                 final String mapKey) throws HBqlException;

    public abstract void setFamilyDefaultCurrentValue(final Object obj,
                                                      final String name,
                                                      final byte[] value) throws HBqlException;

    public abstract void setFamilyDefaultVersionMap(final Object obj,
                                                    final String name,
                                                    final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException;


    public abstract void setFamilyDefaultKeysAsColumnsValue(final Object obj,
                                                            final String columnName,
                                                            final String mapKey,
                                                            final byte[] valueBytes) throws HBqlException;

    public abstract void setFamilyDefaultKeysAsColumnsVersionMap(final Object obj,
                                                                 final String columnName,
                                                                 final String mapKey, final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException;


    public void setKeysAsColumnsVersionMap(final Object obj,
                                           final String mapKey,
                                           final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException {

        final Map<Long, Object> mapVal = this.getKeysAsColumnsVersionMap(obj, mapKey);

        for (final Long timestamp : timeStampMap.keySet()) {
            final Object val = this.getValueFromBytes(obj, timeStampMap.get(timestamp));
            mapVal.put(timestamp, val);
        }
    }

    public void setVersionMap(final Object obj, final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException {

        final Map<Long, Object> mapVal = this.getVersionMap(obj);

        for (final Long timestamp : timeStampMap.keySet()) {
            final Object val = this.getValueFromBytes(obj, timeStampMap.get(timestamp));
            mapVal.put(timestamp, val);
        }
    }

    protected abstract Method getMethod(final String methodName,
                                        final Class<?>... params) throws NoSuchMethodException, HBqlException;

    protected abstract Class getComponentType() throws HBqlException;

    public abstract String getNameToUseInExceptions();

    public abstract String getEnclosingClassName();

    public String getFamilyQualifiedName() {
        if (this.getFamilyName() != null && this.getFamilyName().length() > 0)
            return this.getFamilyName() + ":" + this.getColumnName();
        else
            return this.getColumnName();
    }

    public byte[] getFamilyNameBytes() throws HBqlException {
        if (this.familyBytes != null)
            return this.familyBytes;

        this.familyBytes = HUtil.getSerialization().getStringAsBytes(this.getFamilyName());
        return this.familyBytes;
    }

    public byte[] getColumnNameBytes() throws HBqlException {

        if (this.columnBytes != null)
            return this.columnBytes;

        this.columnBytes = HUtil.getSerialization().getStringAsBytes(this.getColumnName());
        return this.columnBytes;
    }

    public boolean equals(final Object o) {

        if (!(o instanceof ColumnAttrib))
            return false;

        final ColumnAttrib var = (ColumnAttrib)o;

        return var.getAliasName().equals(this.getAliasName())
               && var.getFamilyQualifiedName().equals(this.getFamilyQualifiedName());
    }

    protected void defineAccessors() throws HBqlException {
        try {
            if (this.getGetter() != null && this.getGetter().length() > 0) {
                this.getterMethod = this.getMethod(this.getGetter());

                // Check return type of getter
                final Class<?> returnType = this.getGetterMethod().getReturnType();

                if (!(returnType.isArray() && returnType.getComponentType() == Byte.TYPE))
                    throw new HBqlException(this.getEnclosingClassName() + "." + this.getGetter() + "()"
                                            + " does not have a return type of byte[]");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HBqlException("Missing method byte[] " + this.getEnclosingClassName() + "."
                                    + this.getGetter() + "()");
        }

        try {
            if (this.getSetter() != null && this.getSetter().length() > 0) {
                this.setterMethod = this.getMethod(this.getSetter(), Class.forName("[B"));

                // Check if it takes single byte[] arg
                final Class<?>[] args = this.getSetterMethod().getParameterTypes();
                if (args.length != 1 || !(args[0].isArray() && args[0].getComponentType() == Byte.TYPE))
                    throw new HBqlException(this.getEnclosingClassName() + "." + this.getSetter() + "()"
                                            + " does not have single byte[] arg");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HBqlException("Missing method " + this.getEnclosingClassName()
                                    + "." + this.getSetter() + "(byte[] arg)");
        }
        catch (ClassNotFoundException e) {
            // This will not be hit
            throw new HBqlException("Missing method " + this.getEnclosingClassName()
                                    + "." + this.getSetter() + "(byte[] arg)");
        }
    }

    public byte[] invokeGetterMethod(final Object obj) throws HBqlException {
        try {
            return (byte[])this.getGetterMethod().invoke(obj);
        }
        catch (IllegalAccessException e) {
            throw new HBqlException("Error getting value of " + this.getNameToUseInExceptions());
        }
        catch (InvocationTargetException e) {
            throw new HBqlException("Error getting value of " + this.getNameToUseInExceptions());
        }
    }

    public Object invokeSetterMethod(final Object obj, final byte[] b) throws HBqlException {
        try {
            // TODO Resolve passing primitive to Object varargs
            return this.getSetterMethod().invoke(obj, b);
        }
        catch (IllegalAccessException e) {
            throw new HBqlException("Error setting value of " + this.getNameToUseInExceptions());
        }
        catch (InvocationTargetException e) {
            throw new HBqlException("Error setting value of " + this.getNameToUseInExceptions());
        }
    }

    public byte[] getValueAsBytes(final Object obj) throws HBqlException {

        if (this.hasGetter())
            return this.invokeGetterMethod(obj);

        final Object value = this.getCurrentValue(obj);

        if (this.isArray())
            return HUtil.getSerialization().getArrayasBytes(this.getFieldType(), value);
        else
            return HUtil.getSerialization().getScalarAsBytes(this.getFieldType(), value);
    }

    public Object getValueFromBytes(final Object obj, final byte[] b) throws HBqlException {

        if (this.hasSetter())
            return this.invokeSetterMethod(obj, b);

        if (this.isArray())
            return HUtil.getSerialization().getArrayFromBytes(this.getFieldType(), this.getComponentType(), b);
        else
            return HUtil.getSerialization().getScalarFromBytes(this.getFieldType(), b);
    }

    public Object getValueFromBytes(final Result result) throws HBqlException, ResultMissingColumnException {

        if (this.isKeyAttrib()) {
            return HUtil.getSerialization().getStringFromBytes(result.getRow());
        }
        else {

            if (!result.containsColumn(this.getFamilyNameBytes(), this.getColumnNameBytes())) {
                // See if a default value is present
                final Object defaultValue = this.getDefaultValue();
                if (defaultValue != null)
                    return defaultValue;
                else
                    throw new ResultMissingColumnException(this.getFamilyQualifiedName());
            }

            final byte[] b = result.getValue(this.getFamilyNameBytes(), this.getColumnNameBytes());

            if (this.isArray())
                return HUtil.getSerialization().getArrayFromBytes(this.getFieldType(), this.getComponentType(), b);
            else
                return HUtil.getSerialization().getScalarFromBytes(this.getFieldType(), b);
        }
    }

    public void setCurrentValue(final Object obj, final long timestamp, final byte[] b) throws HBqlException {
        final Object val = this.getValueFromBytes(obj, b);
        this.setCurrentValue(obj, timestamp, val);
    }

    public byte[] getFamilyNameAsBytes() throws HBqlException {
        return HUtil.getSerialization().getStringAsBytes(this.getFamilyName());
    }

    public byte[] getColumnNameAsBytes() throws HBqlException {
        return HUtil.getSerialization().getStringAsBytes(this.getColumnName());
    }

    protected String getGetter() {
        return this.getter;
    }

    protected String getSetter() {
        return this.setter;
    }

    protected Method getGetterMethod() {
        return this.getterMethod;
    }

    protected Method getSetterMethod() {
        return this.setterMethod;
    }

    public boolean isMapKeysAsColumnsAttrib() {
        return this.mapKeysAsColumns;
    }

    public boolean isFamilyDefaultAttrib() {
        return this.familyDefault;
    }

    protected boolean hasGetter() {
        return this.getGetterMethod() != null;
    }

    protected boolean hasSetter() {
        return this.getSetterMethod() != null;
    }

    public boolean isACurrentValue() {
        return true;
    }

    public boolean isAVersionValue() {
        return false;
    }

    public String getAliasName() {
        return (this.aliasName != null
                && this.aliasName.length() > 0) ? this.aliasName : this.getFamilyQualifiedName();
    }

    public boolean isASelectFamilyAttrib() {
        return false;
    }

    public boolean isArray() {
        return this.isArray;
    }

    public String getFamilyName() {
        return this.familyName;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public FieldType getFieldType() {
        return this.fieldType;
    }

    public boolean isKeyAttrib() {
        return false;
    }
}
