package com.imap4j.hbase.hbql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

        private static Type getType(final Field field) throws PersistException {

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

            throw new PersistException("Not dealing with type: " + clazz);
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


    public FieldAttrib(final Class enclosingClass, final Field field, final Column column) throws PersistException {

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
                    throw new PersistException(enclosingClass.getName() + "." + this.getter + "()"
                                               + " does not have a return type of byte[]");
            }
        }
        catch (NoSuchMethodException e) {
            throw new PersistException("Missing method byte[] " + enclosingClass.getName() + "." + this.getter + "()");
        }

        try {
            if (this.hasSetter()) {

                this.setterMethod = enclosingClass.getDeclaredMethod(this.setter, Class.forName("[B"));

                // Check if it takes single byte[] arg
                final Class<?>[] args = this.getSetterMethod().getParameterTypes();
                if (args.length != 1 || !(args[0].isArray() && args[0].getComponentType() == Byte.TYPE))
                    throw new PersistException(enclosingClass.getName() + "." + this.setter + "()"
                                               + " does not have single byte[] arg");
            }
        }
        catch (NoSuchMethodException e) {
            throw new PersistException("Missing method " + enclosingClass.getName() + "." + this.setter + "(byte[] arg)");
        }
        catch (ClassNotFoundException e) {
            // This will not be hit
            throw new PersistException("Missing method " + enclosingClass.getName() + "." + this.setter + "(byte[] arg)");
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

    public boolean isMapKeysAsColumns() {
        return this.mapKeysAsColumns;
    }

    public byte[] invokeGetterMethod(final Object parent) throws PersistException {
        try {
            return (byte[])this.getGetterMethod().invoke(parent);
        }
        catch (IllegalAccessException e) {
            throw new PersistException("Error getting value of " + this.getFieldName());
        }
        catch (InvocationTargetException e) {
            throw new PersistException("Error getting value of " + this.getFieldName());
        }
    }

    public Object getValue(final Persistable declaringObj) throws PersistException {
        try {
            return this.getField().get(declaringObj);
        }
        catch (IllegalAccessException e) {
            throw new PersistException("Error getting value of " + this.getFieldName());
        }

    }

    public byte[] getValueAsBytes(final Persistable declaringObj) throws PersistException, IOException {

        if (this.hasGetter()) {
            return this.invokeGetterMethod(declaringObj);
        }
        else {
            final Object instanceObj = this.getValue(declaringObj);
            return this.asBytes(instanceObj);
        }
    }


    public byte[] asBytes(final Object obj) throws IOException, PersistException {

        if (obj.getClass().isArray())
            return this.getArrayasBytes(obj);
        else
            return this.getScalarAsBytes(obj);
    }

    public Object getScalarfromBytes(final byte[] b) throws IOException, PersistException {

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
            throw new PersistException("Error in getScalarfromBytes()");
        }
        finally {
            ois.close();
        }

        throw new PersistException("Error in getScalarfromBytes()");
    }

    private byte[] getScalarAsBytes(final Object obj) throws IOException, PersistException {

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

    private byte[] getArrayasBytes(final Object obj) throws IOException, PersistException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);

        switch (this.getComponentType()) {

            case BooleanType: {
                final boolean[] val = (boolean[])obj;
                for (int i = 0; i < val.length; i++) oos.writeBoolean(val[i]);
                break;
            }

            case ByteType: {
                final byte[] val = (byte[])obj;
                for (int i = 0; i < val.length; i++) oos.write(val[i]);
                break;
            }

            case CharType: {
                final char[] val = (char[])obj;
                for (int i = 0; i < val.length; i++) oos.write(val[i]);
                break;
            }

            case ShortType: {
                final short[] val = (short[])obj;
                for (int i = 0; i < val.length; i++) oos.writeShort(val[i]);
                break;
            }

            case IntegerType: {
                final int[] val = (int[])obj;
                for (int i = 0; i < val.length; i++) oos.writeInt(val[i]);
                break;
            }

            case LongType: {
                final long[] val = (long[])obj;
                for (int i = 0; i < val.length; i++) oos.writeLong(val[i]);
                break;
            }

            case FloatType: {
                final float[] val = (float[])obj;
                for (int i = 0; i < val.length; i++) oos.writeFloat(val[i]);
                break;
            }

            case DoubleType: {
                final double[] val = (double[])obj;
                for (int i = 0; i < val.length; i++) oos.writeDouble(val[i]);
                break;
            }

            case ObjectType: {
                final Object[] val = (Object[])obj;
                for (int i = 0; i < val.length; i++) oos.writeObject(val[i]);
                break;
            }
        }

        oos.flush();
        return baos.toByteArray();

    }

}
