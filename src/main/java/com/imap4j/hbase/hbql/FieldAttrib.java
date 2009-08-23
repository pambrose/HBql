package com.imap4j.hbase.hbql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 6:07:31 PM
 */
public class FieldAttrib {

    private enum Type {

        BooleanType(Boolean.TYPE),
        ByteType(Byte.TYPE),
        CharType(Character.TYPE),
        ShortType(Short.TYPE),
        IntegerType(Integer.TYPE),
        LongType(Long.TYPE),
        FloatType(Float.TYPE),
        DoubleType(Double.TYPE),
        ObjectType(Object.class);

        private final Class clazz;

        private Type(final Class clazz) {
            this.clazz = clazz;
        }

        private Class getClazz() {
            return clazz;
        }

        private static Type getType(final Field field) throws HBPersistException {

            final Class fieldClass = field.getType();

            final Class<?> clazz = fieldClass.isArray() ? fieldClass.getComponentType() : fieldClass;

            if (!clazz.isPrimitive()) {
                return ObjectType;
            }
            else {
                for (final Type type : values())
                    if (clazz == type.getClazz())
                        return type;
            }

            throw new HBPersistException("Not able to deal with type: " + clazz);
        }
    }

    private final Field field;
    private final Type type;
    private final String familyName;
    private final String columnName;
    private final String getter;
    private final String setter;
    private final boolean key;
    private final boolean mapKeysAsColumns;

    private Method getterMethod = null;
    private Method setterMethod = null;


    public FieldAttrib(final Class enclosingClass, final Field field, final HBColumn column) throws HBPersistException {

        this.field = field;
        this.type = Type.getType(this.field);

        this.familyName = column.family();
        this.columnName = column.column().length() > 0 ? column.column() : this.getFieldName();
        this.getter = column.getter();
        this.setter = column.setter();
        this.key = column.key();
        this.mapKeysAsColumns = column.mapKeysAsColumns();

        try {
            if (this.hasGetter()) {
                this.getterMethod = enclosingClass.getDeclaredMethod(this.getter);

                // Check return type of getter
                final Class<?> returnType = this.getGetterMethod().getReturnType();

                if (!(returnType.isArray() && returnType.getComponentType() == Byte.TYPE))
                    throw new HBPersistException(enclosingClass.getName() + "." + this.getter + "()"
                                                 + " does not have a return type of byte[]");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HBPersistException("Missing method byte[] " + enclosingClass.getName() + "." + this.getter + "()");
        }

        try {
            if (this.hasSetter()) {

                this.setterMethod = enclosingClass.getDeclaredMethod(this.setter, Class.forName("[B"));

                // Check if it takes single byte[] arg
                final Class<?>[] args = this.getSetterMethod().getParameterTypes();
                if (args.length != 1 || !(args[0].isArray() && args[0].getComponentType() == Byte.TYPE))
                    throw new HBPersistException(enclosingClass.getName() + "." + this.setter + "()"
                                                 + " does not have single byte[] arg");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HBPersistException("Missing method " + enclosingClass.getName() + "." + this.setter + "(byte[] arg)");
        }
        catch (ClassNotFoundException e) {
            // This will not be hit
            throw new HBPersistException("Missing method " + enclosingClass.getName() + "." + this.setter + "(byte[] arg)");
        }

    }

    @Override
    public String toString() {
        return this.getField().getDeclaringClass() + "." + this.getFieldName();
    }

    public String getFieldName() {
        return this.getField().getName();
    }

    public boolean isKey() {
        return key;
    }

    public Type getComponentType() {
        return type;
    }

    private Method getGetterMethod() {
        return this.getterMethod;
    }

    private Method getSetterMethod() {
        return this.setterMethod;
    }

    public boolean hasGetter() {
        return this.getter.length() > 0;
    }

    public boolean hasSetter() {
        return this.setter.length() > 0;
    }

    public String getFamilyName() {
        return this.familyName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getQualifiedName() {
        return this.getFamilyName() + ":" + this.getColumnName();
    }

    public Field getField() {
        return field;
    }

    public boolean isArray() {
        return this.getField().getType().isArray();
    }

    public boolean isMapKeysAsColumns() {
        return this.mapKeysAsColumns;
    }

    public byte[] invokeGetterMethod(final Object parent) throws HBPersistException {
        try {
            return (byte[])this.getGetterMethod().invoke(parent);
        }
        catch (IllegalAccessException e) {
            throw new HBPersistException("Error getting value of " + this.getFieldName());
        }
        catch (InvocationTargetException e) {
            throw new HBPersistException("Error getting value of " + this.getFieldName());
        }
    }

    public Object invokeSetterMethod(final Object parent, final byte[] b) throws HBPersistException {
        try {
            return this.getSetterMethod().invoke(parent, b);
        }
        catch (IllegalAccessException e) {
            throw new HBPersistException("Error setting value of " + this.getFieldName());
        }
        catch (InvocationTargetException e) {
            throw new HBPersistException("Error setting value of " + this.getFieldName());
        }
    }

    public Object getValue(final HBPersistable declaringObj) throws HBPersistException {
        try {
            return this.getField().get(declaringObj);
        }
        catch (IllegalAccessException e) {
            throw new HBPersistException("Error getting value of " + this.getFieldName());
        }

    }

    public byte[] getValueAsBytes(final HBPersistable declaringObj) throws HBPersistException, IOException {

        if (this.hasGetter()) {
            return this.invokeGetterMethod(declaringObj);
        }
        else {
            final Object obj = this.getValue(declaringObj);

            if (this.isArray())
                return this.getArrayasBytes(obj);
            else
                return this.getScalarAsBytes(obj);
        }
    }

    public Object getValueFromBytes(final HBPersistable declaringObj, final byte[] b) throws IOException, HBPersistException {

        if (this.hasSetter()) {
            return this.invokeSetterMethod(declaringObj, b);
        }
        else {
            if (this.isArray())
                return this.getArrayFromBytes(b);
            else
                return this.getScalarFromBytes(b);
        }
    }

    private Object getScalarFromBytes(final byte[] b) throws IOException, HBPersistException {

        final ByteArrayInputStream bais = new ByteArrayInputStream(b);
        final ObjectInputStream ois = new ObjectInputStream(bais);

        try {
            switch (this.getComponentType()) {

                case BooleanType:
                    return ois.readBoolean();

                case ByteType:
                    return ois.readByte();

                case CharType:
                    return ois.readByte();

                case ShortType:
                    return ois.readShort();

                case IntegerType:
                    return ois.readInt();

                case LongType:
                    return ois.readLong();

                case FloatType:
                    return ois.readFloat();

                case DoubleType:
                    return ois.readDouble();

                case ObjectType:
                    return ois.readObject();
            }
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new HBPersistException("Error in getScalarfromBytes()");
        }
        finally {
            ois.close();
        }

        throw new HBPersistException("Error in getScalarfromBytes()");
    }

    private Object getArrayFromBytes(final byte[] b) throws IOException, HBPersistException {

        final ByteArrayInputStream bais = new ByteArrayInputStream(b);
        final ObjectInputStream ois = new ObjectInputStream(bais);

        try {
            final int length = ois.readInt();

            switch (this.getComponentType()) {

                case BooleanType: {
                    final Object array = Array.newInstance(Boolean.TYPE, length);
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readBoolean());
                    return array;
                }

                case ByteType: {
                    final Object array = Array.newInstance(Byte.TYPE, length);
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readByte());
                    return array;
                }

                case CharType: {
                    final Object array = Array.newInstance(Character.TYPE, length);
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readByte());
                    return array;
                }

                case ShortType: {
                    final Object array = Array.newInstance(Short.TYPE, length);
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readShort());
                    return array;
                }

                case IntegerType: {
                    final Object array = Array.newInstance(Integer.TYPE, length);
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readInt());
                    return array;
                }

                case LongType: {
                    final Object array = Array.newInstance(Long.TYPE, length);
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readLong());
                    return array;
                }

                case FloatType: {
                    final Object array = Array.newInstance(Float.TYPE, length);
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readFloat());
                    return array;
                }

                case DoubleType: {
                    final Object array = Array.newInstance(Double.TYPE, length);
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readDouble());
                    return array;
                }

                case ObjectType: {
                    final String className = this.getField().getType().getComponentType().getName();
                    final Class clazz = Class.forName(className);
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        final Object obj = ois.readObject();
                        Array.set(array, i, obj);
                    }
                    return array;
                }
            }
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new HBPersistException("Error in getScalarfromBytes()");
        }
        finally {
            ois.close();
        }

        throw new HBPersistException("Error in getScalarfromBytes()");
    }

    private byte[] getScalarAsBytes(final Object obj) throws IOException, HBPersistException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);

        switch (this.getComponentType()) {

            case BooleanType:
                oos.writeBoolean((Boolean)obj);
                break;

            case ByteType:
                oos.writeByte((Byte)obj);
                break;

            case CharType:
                oos.writeByte((Character)obj);
                break;

            case ShortType:
                oos.writeShort((Short)obj);
                break;

            case IntegerType:
                oos.writeInt((Integer)obj);
                break;

            case LongType:
                oos.writeLong((Long)obj);
                break;

            case FloatType:
                oos.writeFloat((Float)obj);
                break;

            case DoubleType:
                oos.writeDouble((Double)obj);
                break;

            case ObjectType:
                oos.writeObject(obj);
                break;
        }

        oos.flush();
        return baos.toByteArray();
    }

    private byte[] getArrayasBytes(final Object obj) throws IOException, HBPersistException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);

        switch (this.getComponentType()) {

            case BooleanType: {
                oos.writeInt(((boolean[])obj).length);
                for (boolean val : (boolean[])obj)
                    oos.writeBoolean(val);
                break;
            }

            case ByteType: {
                oos.writeInt(((byte[])obj).length);
                for (byte val : (byte[])obj)
                    oos.write(val);
                break;
            }

            case CharType: {
                oos.writeInt(((char[])obj).length);
                for (char val : (char[])obj)
                    oos.write(val);
                break;
            }

            case ShortType: {
                oos.writeInt(((short[])obj).length);
                for (short val : (short[])obj)
                    oos.writeShort(val);
                break;
            }

            case IntegerType: {
                oos.writeInt(((int[])obj).length);
                for (int val : (int[])obj)
                    oos.writeInt(val);
                break;
            }

            case LongType: {
                oos.writeInt(((long[])obj).length);
                for (long val : (long[])obj)
                    oos.writeLong(val);
                break;
            }

            case FloatType: {
                oos.writeInt(((float[])obj).length);
                for (float val : (float[])obj)
                    oos.writeFloat(val);
                break;
            }

            case DoubleType: {
                oos.writeInt(((double[])obj).length);
                for (double val : (double[])obj)
                    oos.writeDouble(val);
                break;
            }

            case ObjectType: {
                oos.writeInt(((Object[])obj).length);
                for (Object val : (Object[])obj)
                    oos.writeObject(val);
                break;
            }
        }

        oos.flush();
        return baos.toByteArray();

    }

}
