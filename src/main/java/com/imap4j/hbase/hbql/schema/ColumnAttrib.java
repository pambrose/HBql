package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.io.Serialization;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:27:00 PM
 */
public abstract class ColumnAttrib extends VariableAttrib {

    private final FieldType fieldType;
    private final String family, column, getter, setter;
    private final boolean mapKeysAsColumns;

    private final transient Field field;
    protected transient Method getterMethod = null, setterMethod = null;

    public ColumnAttrib(final Field field,
                        final FieldType fieldType,
                        final String family,
                        final String column,
                        final String getter,
                        final String setter,
                        final boolean mapKeysAsColumns) throws HPersistException {
        this.field = field;
        this.fieldType = fieldType;
        this.family = family;
        this.column = column;
        this.getter = getter;
        this.setter = setter;
        this.mapKeysAsColumns = mapKeysAsColumns;

        try {
            if (this.getGetter().length() > 0) {
                this.getterMethod = this.getEnclosingClass().getDeclaredMethod(this.getGetter());

                // Check return type of getter
                final Class<?> returnType = this.getGetterMethod().getReturnType();

                if (!(returnType.isArray() && returnType.getComponentType() == Byte.TYPE))
                    throw new HPersistException(this.getEnclosingClass().getName()
                                                + "." + this.getGetter() + "()"
                                                + " does not have a return type of byte[]");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HPersistException("Missing method byte[] " + this.getEnclosingClass().getName() + "."
                                        + this.getGetter() + "()");
        }

        try {
            if (this.getSetter().length() > 0) {
                this.setterMethod = this.getEnclosingClass().getDeclaredMethod(this.getSetter(), Class.forName("[B"));

                // Check if it takes single byte[] arg
                final Class<?>[] args = this.getSetterMethod().getParameterTypes();
                if (args.length != 1 || !(args[0].isArray() && args[0].getComponentType() == Byte.TYPE))
                    throw new HPersistException(this.getEnclosingClass().getName()
                                                + "." + this.getSetter() + "()" + " does not have single byte[] arg");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HPersistException("Missing method " + this.getEnclosingClass().getName()
                                        + "." + this.getSetter() + "(byte[] arg)");
        }
        catch (ClassNotFoundException e) {
            // This will not be hit
            throw new HPersistException("Missing method " + this.getEnclosingClass().getName()
                                        + "." + this.getSetter() + "(byte[] arg)");
        }
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

    protected Class getEnclosingClass() {
        return this.getField().getDeclaringClass();
    }

    public FieldType getFieldType() {
        return this.fieldType;
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

    public String getFamilyQualifiedName() {
        return this.getFamilyName() + ":" + this.getColumnName();
    }

    public String getFamilyName() {
        return this.family;
    }

    public String getColumnName() {
        return this.column.length() > 0 ? this.column : this.getVariableName();
    }

    protected Field getField() {
        return this.field;
    }

    public boolean isMapKeysAsColumns() {
        return this.mapKeysAsColumns;
    }

    public boolean isArray() {
        return this.getField().getType().isArray();
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

    public Object getValue(final HPersistable recordObj) throws HPersistException {
        try {
            return this.getField().get(recordObj);
        }
        catch (IllegalAccessException e) {
            throw new HPersistException("Error getting value of " + this.getObjectQualifiedName());
        }

    }

    public byte[] getValueAsBytes(final Serialization ser,
                                  final HPersistable recordObj) throws HPersistException, IOException {

        if (this.hasGetter()) {
            return this.invokeGetterMethod(recordObj);
        }
        else {
            final Object obj = this.getValue(recordObj);

            if (this.isArray())
                return ser.getArrayasBytes(this.getFieldType(), obj);
            else
                return ser.getScalarAsBytes(this.getFieldType(), obj);
        }
    }

    public Object getValueFromBytes(final Serialization ser,
                                    final HPersistable recordObj,
                                    final byte[] b) throws IOException, HPersistException {

        if (this.hasSetter()) {
            return this.invokeSetterMethod(recordObj, b);
        }
        else {
            if (this.isArray())
                return ser.getArrayFromBytes(this.getFieldType(), this.getField().getType().getComponentType(), b);
            else
                return ser.getScalarFromBytes(this.getFieldType(), b);
        }
    }

    public void setValue(final HPersistable newobj, final Object val) {
        try {
            this.getField().set(newobj, val);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException("Error setting value of " + this.getObjectQualifiedName());
        }

    }

    public void setValue(final Serialization ser,
                         final HPersistable newobj,
                         final byte[] b) throws IOException, HPersistException {
        final Object val = this.getValueFromBytes(ser, newobj, b);
        this.setValue(newobj, val);
    }


    protected static boolean isFinal(final Field field) {

        final boolean isFinal = Modifier.isFinal(field.getModifiers());

        if (isFinal)
            return true;

        // Unlock private vars
        if (!field.isAccessible())
            field.setAccessible(true);

        return false;
    }

}
