package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 6:07:31 PM
 */
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
    protected transient Method getterMethod = null;
    protected transient Method setterMethod = null;

    protected ColumnAttrib(final String familyName,
                           final String columnName,
                           final String aliasName,
                           final FieldType fieldType,
                           final boolean mapKeysAsColumns,
                           final String getter,
                           final String setter) {
        this.familyName = familyName;
        this.columnName = columnName;
        this.aliasName = aliasName;
        this.fieldType = fieldType;
        this.mapKeysAsColumns = mapKeysAsColumns;
        this.getter = getter;
        this.setter = setter;
    }

    public abstract boolean isArray();

    public String getFamilyName() {
        return this.familyName;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public String getFamilyQualifiedName() {
        if (this.getFamilyName() != null && this.getFamilyName().length() > 0)
            return this.getFamilyName() + ":" + this.getColumnName();
        else
            return this.getColumnName();
    }

    public String getAliasName() {
        return (this.aliasName != null
                && this.aliasName.length() > 0) ? this.aliasName : this.getFamilyQualifiedName();
    }

    public abstract Object getCurrentValue(final Object recordObj) throws HBqlException;

    protected abstract void setCurrentValue(final Object newobj, final long timestamp, final Object val) throws HBqlException;

    public abstract Object getVersionedValueMap(final Object recordObj) throws HBqlException;

    protected abstract void setVersionedValueMap(final Object newobj, final Map<Long, Object> map);

    public FieldType getFieldType() {
        return this.fieldType;
    }

    public boolean isKeyAttrib() {
        return false;
    }

    public byte[] getFamilyNameBytes() throws HBqlException {
        if (this.familyBytes != null)
            return this.familyBytes;

        this.familyBytes = HUtil.ser.getStringAsBytes(this.getFamilyName());
        return this.familyBytes;
    }

    public byte[] getColumnNameBytes() throws HBqlException {
        if (this.columnBytes != null)
            return this.columnBytes;

        this.columnBytes = HUtil.ser.getStringAsBytes(this.getColumnName());
        return this.columnBytes;
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof ColumnAttrib))
            return false;

        final ColumnAttrib var = (ColumnAttrib)o;

        return var.getColumnName().equals(this.getColumnName())
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

    public boolean isACurrentValue() {
        return true;
    }

    protected abstract Method getMethod(final String methodName, final Class<?>... params) throws NoSuchMethodException;

    protected abstract Class getComponentType();

    public abstract String getObjectQualifiedName();

    public abstract String getEnclosingClassName();

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

    public boolean isMapKeysAsColumns() {
        return this.mapKeysAsColumns;
    }

    protected boolean hasGetter() {
        return this.getGetterMethod() != null;
    }

    protected boolean hasSetter() {
        return this.getSetterMethod() != null;
    }

    public byte[] invokeGetterMethod(final Object recordObj) throws HBqlException {
        try {
            return (byte[])this.getGetterMethod().invoke(recordObj);
        }
        catch (IllegalAccessException e) {
            throw new HBqlException("Error getting value of " + this.getObjectQualifiedName());
        }
        catch (InvocationTargetException e) {
            throw new HBqlException("Error getting value of " + this.getObjectQualifiedName());
        }
    }

    public Object invokeSetterMethod(final Object recordObj, final byte[] b) throws HBqlException {
        try {
            // TODO Resolve passing primitive to Object varargs
            return this.getSetterMethod().invoke(recordObj, b);
        }
        catch (IllegalAccessException e) {
            throw new HBqlException("Error setting value of " + this.getObjectQualifiedName());
        }
        catch (InvocationTargetException e) {
            throw new HBqlException("Error setting value of " + this.getObjectQualifiedName());
        }
    }

    public byte[] getValueAsBytes(final Object recordObj) throws HBqlException {

        if (this.hasGetter()) {
            return this.invokeGetterMethod(recordObj);
        }
        else {
            final Object obj = this.getCurrentValue(recordObj);

            if (this.isArray())
                return HUtil.ser.getArrayasBytes(this.getFieldType(), obj);
            else
                return HUtil.ser.getScalarAsBytes(this.getFieldType(), obj);
        }
    }

    public Object getValueFromBytes(final Object recordObj, final byte[] b) throws HBqlException {

        if (this.hasSetter()) {
            return this.invokeSetterMethod(recordObj, b);
        }
        else {
            if (this.isArray())
                return HUtil.ser.getArrayFromBytes(this.getFieldType(), this.getComponentType(), b);
            else
                return HUtil.ser.getScalarFromBytes(this.getFieldType(), b);
        }
    }

    public Object getValueFromBytes(final Result result) throws HBqlException {

        if (this.isKeyAttrib()) {
            final byte[] b = result.getRow();
            return HUtil.ser.getStringFromBytes(b);
        }

        final NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyMap = result.getNoVersionMap();

        final NavigableMap<byte[], byte[]> columnMap = familyMap.get(this.getFamilyNameBytes());
        if (columnMap == null)
            throw new HBqlException("Invalid family name: " + this.getFamilyName());

        final byte[] b = columnMap.get(this.getColumnNameBytes());

        if (this.isArray())
            return HUtil.ser.getArrayFromBytes(this.getFieldType(), this.getComponentType(), b);
        else
            return HUtil.ser.getScalarFromBytes(this.getFieldType(), b);
    }

    public void setCurrentValue(final Object newobj,
                                final long timestamp,
                                final byte[] b) throws HBqlException {
        final Object val = this.getValueFromBytes(newobj, b);
        this.setCurrentValue(newobj, timestamp, val);
    }

    public byte[] getFamilyNameAsBytes() throws HBqlException {
        return HUtil.ser.getStringAsBytes(this.getFamilyName());
    }

    public byte[] getColumnNameAsBytes() throws HBqlException {
        return HUtil.ser.getStringAsBytes(this.getColumnName());
    }
}
