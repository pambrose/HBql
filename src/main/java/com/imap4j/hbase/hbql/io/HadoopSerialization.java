package com.imap4j.hbase.hbql.io;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.schema.FieldType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:18:29 PM
 */
public class HadoopSerialization extends Serialization {

    @Override
    public byte[] getObjectAsBytes(final Object obj) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.flush();
        return baos.toByteArray();
    }

    @Override
    public Object getObjectFromBytes(final byte[] b) throws IOException, HPersistException {
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

    @Override
    public Object getScalarFromBytes(final FieldType fieldType, final byte[] b) throws IOException, HPersistException {

        final ByteArrayInputStream bais = new ByteArrayInputStream(b);
        final ObjectInputStream ois = new ObjectInputStream(bais);

        try {
            switch (fieldType) {

                case BooleanType:
                    return Bytes.toBoolean(b);

                case ByteType:
                    return Bytes.toShort(b);

                case CharType:
                    return Bytes.toShort(b);

                case ShortType:
                    return Bytes.toShort(b);

                case IntegerType:
                    return Bytes.toInt(b);

                case LongType:
                    return Bytes.toLong(b);

                case FloatType:
                    return Bytes.toFloat(b);

                case DoubleType:
                    return Bytes.toDouble(b);

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

    @Override
    public byte[] getScalarAsBytes(final FieldType fieldType, final Object obj) throws IOException, HPersistException {

        switch (fieldType) {

            case BooleanType:
                return Bytes.toBytes((Boolean)obj);

            case ByteType:
                return Bytes.toBytes((Short)obj);

            case CharType:
                return Bytes.toBytes((Short)obj);

            case ShortType:
                return Bytes.toBytes((Short)obj);

            case IntegerType:
                return Bytes.toBytes((Integer)obj);

            case LongType:
                return Bytes.toBytes((Long)obj);

            case FloatType:
                return Bytes.toBytes((Float)obj);

            case DoubleType:
                return Bytes.toBytes((Double)obj);

            case ObjectType:
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(obj);
                oos.flush();
                return baos.toByteArray();
        }

        throw new HPersistException("Error in getScalarfromBytes()");
    }

    @Override
    public Object getArrayFromBytes(final FieldType fieldType, final Class clazz, final byte[] b) throws IOException, HPersistException {

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

    @Override
    public byte[] getArrayasBytes(final FieldType fieldType, final Object obj) throws IOException, HPersistException {

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