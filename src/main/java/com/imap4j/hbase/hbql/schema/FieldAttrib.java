package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HColumn;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.io.Serialization;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 6:07:31 PM
 */
public class FieldAttrib implements Serializable {

    private final String name;
    private final HColumn columnAnnotation;
    private final FieldType fieldType;

    private final transient Field field;
    private transient Method getterMethod = null, setterMethod = null;

    public FieldAttrib(final String name, final FieldType type) {
        this.field = null;
        this.name = name;
        this.fieldType = type;
        this.columnAnnotation = null;
    }

    public FieldAttrib(final Class enclClass, final Field field, final HColumn columnAnnotation) throws HPersistException {
        this.field = field;
        this.name = this.getField().getName();
        this.fieldType = FieldType.getFieldType(this.field);
        this.columnAnnotation = columnAnnotation;

        try {
            if (this.hasGetter()) {
                this.getterMethod = enclClass.getDeclaredMethod(this.getColumnAnno().getter());

                // Check return type of getter
                final Class<?> returnType = this.getGetterMethod().getReturnType();

                if (!(returnType.isArray() && returnType.getComponentType() == Byte.TYPE))
                    throw new HPersistException(enclClass.getName()
                                                + "." + this.getColumnAnno().getter() + "()"
                                                + " does not have a return type of byte[]");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HPersistException("Missing method byte[] " + enclClass.getName() + "."
                                        + this.getColumnAnno().getter() + "()");
        }

        try {
            if (this.hasSetter()) {
                this.setterMethod = enclClass.getDeclaredMethod(this.getColumnAnno().setter(), Class.forName("[B"));

                // Check if it takes single byte[] arg
                final Class<?>[] args = this.getSetterMethod().getParameterTypes();
                if (args.length != 1 || !(args[0].isArray() && args[0].getComponentType() == Byte.TYPE))
                    throw new HPersistException(enclClass.getName() + "." + this.getColumnAnno()
                            .setter() + "()"
                                                + " does not have single byte[] arg");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HPersistException("Missing method " + enclClass.getName()
                                        + "." + this.getColumnAnno().setter() + "(byte[] arg)");
        }
        catch (ClassNotFoundException e) {
            // This will not be hit
            throw new HPersistException("Missing method " + enclClass.getName()
                                        + "." + this.getColumnAnno().setter() + "(byte[] arg)");
        }
    }

    @Override
    public String toString() {
        return this.getField().getDeclaringClass() + "." + this.getVariableName();
    }

    public String getVariableName() {
        return this.name; //TODO change this back when you cleanup VarDesc -- this.getField().getName();
    }

    private HColumn getColumnAnno() {
        return this.columnAnnotation;
    }

    public boolean isKey() {
        return this.getColumnAnno().key();
    }

    public FieldType getFieldType() {
        return this.fieldType;
    }

    private Method getGetterMethod() {
        return this.getterMethod;
    }

    private Method getSetterMethod() {
        return this.setterMethod;
    }

    public boolean hasGetter() {
        return this.getColumnAnno().getter().length() > 0;
    }

    public boolean hasSetter() {
        return this.getColumnAnno().setter().length() > 0;
    }

    public String getFamilyName() {
        return this.getColumnAnno().family();
    }

    public String getColumnName() {
        return this.getColumnAnno().column().length() > 0 ? getColumnAnno().column() : this.getVariableName();
    }

    public String getQualifiedName() {
        return this.getFamilyName() + ":" + this.getColumnName();
    }

    private Field getField() {
        return this.field;
    }

    public boolean isArray() {
        return this.getField().getType().isArray();
    }

    public boolean isMapKeysAsColumns() {
        return this.getColumnAnno().mapKeysAsColumns();
    }

    public byte[] invokeGetterMethod(final Object recordObj) throws HPersistException {
        try {
            return (byte[])this.getGetterMethod().invoke(recordObj);
        }
        catch (IllegalAccessException e) {
            throw new HPersistException("Error getting value of " + this.getVariableName());
        }
        catch (InvocationTargetException e) {
            throw new HPersistException("Error getting value of " + this.getVariableName());
        }
    }

    public Object invokeSetterMethod(final Object recordObj, final byte[] b) throws HPersistException {
        try {
            // TODO Resolve passing primitive to Object varargs
            return this.getSetterMethod().invoke(recordObj, b);
        }
        catch (IllegalAccessException e) {
            throw new HPersistException("Error setting value of " + this.getVariableName());
        }
        catch (InvocationTargetException e) {
            throw new HPersistException("Error setting value of " + this.getVariableName());
        }
    }

    public Object getValue(final HPersistable recordObj) throws HPersistException {
        try {
            return this.getField().get(recordObj);
        }
        catch (IllegalAccessException e) {
            throw new HPersistException("Error getting value of " + this.getVariableName());
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
            throw new RuntimeException("Error setting value of " + this.getVariableName());
        }

    }

    public void setValue(final Serialization ser,
                         final HPersistable newobj,
                         final byte[] b) throws IOException, HPersistException {
        final Object val = this.getValueFromBytes(ser, newobj, b);
        this.setValue(newobj, val);
    }
}
