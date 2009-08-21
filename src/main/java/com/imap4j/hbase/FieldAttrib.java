package com.imap4j.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

    public enum ComponentType {
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        ObjectType;

        private static ComponentType getType(final Field field) throws PersistException {

            final Class fieldClass = field.getType();

            final Class<?> clazz = fieldClass.isArray() ? fieldClass.getComponentType() : fieldClass;

            if (clazz.isPrimitive()) {
                if (clazz == Boolean.TYPE)
                    return BooleanType;

                if (clazz == Byte.TYPE)
                    return ByteType;

                if (clazz == Short.TYPE)
                    return ShortType;

                if (clazz == Integer.TYPE)
                    return IntegerType;

                if (clazz == Long.TYPE)
                    return LongType;

                if (clazz == Float.TYPE)
                    return FloatType;

                if (clazz == Double.TYPE)
                    return DoubleType;

                throw new PersistException("Not dealing with type: " + clazz);
            }
            else {
                return ObjectType;
            }
        }
    }

    private final Field field;
    private final ComponentType componentType;
    private final String family;
    private final String column;
    private final String lookup;
    private final Column.Strategy strategy;

    private Method lookupMethod = null;


    public FieldAttrib(final Class enclosingClass, final Field field, final Column column) throws PersistException {

        this.field = field;
        this.componentType = ComponentType.getType(this.field);

        this.family = column.family();
        this.column = column.column().length() > 0 ? column.column() : this.getField().getName();
        this.lookup = column.lookup();
        this.strategy = column.strategy();

        try {
            if (this.isLookupColumn()) {
                this.lookupMethod = enclosingClass.getDeclaredMethod(this.lookup);

                // Check return type and args of lookup method
                final Class<?> retClazz = this.getLookupMethod().getReturnType();

                if (!(retClazz.isArray() && retClazz.getComponentType() == Byte.TYPE))
                    throw new PersistException(enclosingClass.getName() + "." + this.lookup + "()"
                                               + " does not have a return type of byte[]");
            }
        }
        catch (NoSuchMethodException e) {
            throw new PersistException("Missing method " + enclosingClass.getName() + "." + this.lookup + "()");
        }

    }

    @Override
    public String toString() {
        return this.getField().getDeclaringClass() + "." + this.getField().getName();
    }

    public ComponentType getComponentType() {
        return componentType;
    }

    private Method getLookupMethod() {
        return lookupMethod;
    }

    public boolean isLookupColumn() {
        return this.lookup.length() > 0;
    }

    public Column.Strategy getStrategy() {
        return this.strategy;
    }

    public boolean isSerializedArray() {
        return this.strategy == Column.Strategy.SERIALIZED_ARRAY;
    }

    public String getFamily() {
        return this.family;
    }

    public String getColumn() {
        return column;
    }

    public Field getField() {
        return field;
    }

    public byte[] invokeLookupMethod(Object obj) throws IllegalAccessException, InvocationTargetException {
        return (byte[])this.getLookupMethod().invoke(obj);
    }

    public byte[] getArrayasBytes(final Object obj) throws IOException, PersistException {

        byte[] retval = null;
        final Class clazz = obj.getClass();

        if (!clazz.isArray())
            throw new PersistException(this + " is not an array type");

        return this.getBytes(obj);

    }


    private byte[] getBytes(final Object obj) throws IOException {

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
            }
        }

        oos.flush();
        return baos.toByteArray();

    }

}
