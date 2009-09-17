package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.io.Serialization;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:27:00 PM
 */
public abstract class ColumnAttrib extends VariableAttrib {

    private final String family, column, getter, setter;
    private final boolean mapKeysAsColumns;

    protected transient Method getterMethod = null, setterMethod = null;

    public ColumnAttrib(final FieldType fieldType,
                        final String family,
                        final String column,
                        final String getter,
                        final String setter,
                        final boolean mapKeysAsColumns) throws HPersistException {
        super(fieldType);
        this.family = family;
        this.column = column;
        this.getter = getter;
        this.setter = setter;
        this.mapKeysAsColumns = mapKeysAsColumns;
    }

    protected void defineAccessors() throws HPersistException {
        try {
            if (this.getGetter() != null && this.getGetter().length() > 0) {
                this.getterMethod = this.getMethod(this.getGetter());

                // Check return type of getter
                final Class<?> returnType = this.getGetterMethod().getReturnType();

                if (!(returnType.isArray() && returnType.getComponentType() == Byte.TYPE))
                    throw new HPersistException(this.getEnclosingClassName() + "." + this.getGetter() + "()"
                                                + " does not have a return type of byte[]");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HPersistException("Missing method byte[] " + this.getEnclosingClassName() + "."
                                        + this.getGetter() + "()");
        }

        try {
            if (this.getSetter() != null && this.getSetter().length() > 0) {
                this.setterMethod = this.getMethod(this.getSetter(), Class.forName("[B"));

                // Check if it takes single byte[] arg
                final Class<?>[] args = this.getSetterMethod().getParameterTypes();
                if (args.length != 1 || !(args[0].isArray() && args[0].getComponentType() == Byte.TYPE))
                    throw new HPersistException(this.getEnclosingClassName() + "." + this.getSetter() + "()"
                                                + " does not have single byte[] arg");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HPersistException("Missing method " + this.getEnclosingClassName()
                                        + "." + this.getSetter() + "(byte[] arg)");
        }
        catch (ClassNotFoundException e) {
            // This will not be hit
            throw new HPersistException("Missing method " + this.getEnclosingClassName()
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

    public String getFamilyQualifiedName() {
        if (this.getFamilyName().length() > 0)
            return this.getFamilyName() + ":" + this.getColumnName();
        else
            return this.getColumnName();
    }

    public String getFamilyName() {
        return this.family;
    }

    public String getColumnName() {
        return this.column;
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

    public byte[] invokeGetterMethod(final Object recordObj) throws HPersistException {
        try {
            return (byte[])this.getGetterMethod().invoke(recordObj);
        }
        catch (IllegalAccessException e) {
            throw new HPersistException("Error getting value of " + this.getObjectQualifiedName());
        }
        catch (InvocationTargetException e) {
            throw new HPersistException("Error getting value of " + this.getObjectQualifiedName());
        }
    }

    public Object invokeSetterMethod(final Object recordObj, final byte[] b) throws HPersistException {
        try {
            // TODO Resolve passing primitive to Object varargs
            return this.getSetterMethod().invoke(recordObj, b);
        }
        catch (IllegalAccessException e) {
            throw new HPersistException("Error setting value of " + this.getObjectQualifiedName());
        }
        catch (InvocationTargetException e) {
            throw new HPersistException("Error setting value of " + this.getObjectQualifiedName());
        }
    }

    public byte[] getValueAsBytes(final Serialization ser,
                                  final Object recordObj) throws HPersistException, IOException {

        if (this.hasGetter()) {
            return this.invokeGetterMethod(recordObj);
        }
        else {
            final Object obj = this.getCurrentValue(recordObj);

            if (this.isArray())
                return ser.getArrayasBytes(this.getFieldType(), obj);
            else
                return ser.getScalarAsBytes(this.getFieldType(), obj);
        }
    }

    public Object getValueFromBytes(final Serialization ser,
                                    final Object recordObj,
                                    final byte[] b) throws IOException, HPersistException {

        if (this.hasSetter()) {
            return this.invokeSetterMethod(recordObj, b);
        }
        else {
            if (this.isArray())
                return ser.getArrayFromBytes(this.getFieldType(), this.getComponentType(), b);
            else
                return ser.getScalarFromBytes(this.getFieldType(), b);
        }
    }

    public void setCurrentValue(final Serialization ser,
                                final Object newobj,
                                final long timestamp,
                                final byte[] b) throws IOException, HPersistException {
        final Object val = this.getValueFromBytes(ser, newobj, b);
        this.setCurrentValue(newobj, timestamp, val);
    }
}
