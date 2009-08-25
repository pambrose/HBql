package com.imap4j.hbase.hbql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class HUtil {

    public static byte[] getObjectAsBytes(final Object obj) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.flush();
        return baos.toByteArray();
    }

    public static Object getObjectFromBytes(final byte[] b) throws IOException, HPersistException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(b);
        final ObjectInputStream ois = new ObjectInputStream(bais);
        try {
            return ois.readObject();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new HPersistException("Error in getObjectFromBytes()");
        }
    }

    public static Object getScalarFromBytes(final FieldType fieldType, final byte[] b) throws IOException, HPersistException {

        final ByteArrayInputStream bais = new ByteArrayInputStream(b);
        final ObjectInputStream ois = new ObjectInputStream(bais);

        try {
            switch (fieldType) {

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
            throw new HPersistException("Error in getScalarfromBytes()");
        }
        finally {
            ois.close();
        }

        throw new HPersistException("Error in getScalarfromBytes()");
    }

    public static byte[] getScalarAsBytes(final FieldType fieldType, final Object obj) throws IOException, HPersistException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);

        switch (fieldType) {

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

    public static Object getArrayFromBytes(final FieldType fieldType, final Class clazz, final byte[] b) throws IOException, HPersistException {

        final ByteArrayInputStream bais = new ByteArrayInputStream(b);
        final ObjectInputStream ois = new ObjectInputStream(bais);

        try {
            final int length = ois.readInt();
            final Object array = Array.newInstance(clazz, length);

            switch (fieldType) {

                case BooleanType: {
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readBoolean());
                    return array;
                }

                case ByteType: {
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readByte());
                    return array;
                }

                case CharType: {
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readByte());
                    return array;
                }

                case ShortType: {
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readShort());
                    return array;
                }

                case IntegerType: {
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readInt());
                    return array;
                }

                case LongType: {
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readLong());
                    return array;
                }

                case FloatType: {
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readFloat());
                    return array;
                }

                case DoubleType: {
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readDouble());
                    return array;
                }

                case ObjectType: {
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readObject());
                    return array;
                }
            }
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new HPersistException("Error in getScalarfromBytes()");
        }
        finally {
            ois.close();
        }

        throw new HPersistException("Error in getScalarfromBytes()");
    }

    public static byte[] getArrayasBytes(final FieldType fieldType, final Object obj) throws IOException, HPersistException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);

        switch (fieldType) {

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
