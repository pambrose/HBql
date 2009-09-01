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

    private static final int lengthsize = Bytes.SIZEOF_INT;

    @Override
    public Object getScalarFromBytes(final FieldType fieldType, final byte[] b) throws IOException, HPersistException {

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

                case StringType:
                    return Bytes.toString(b);

                case ObjectType: {
                    final ByteArrayInputStream bais = new ByteArrayInputStream(b);
                    final ObjectInputStream ois = new ObjectInputStream(bais);
                    try {
                        return ois.readObject();
                    }
                    finally {
                        ois.close();
                    }
                }
            }
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new HPersistException("Error in getScalarfromBytes()");
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

            case StringType:
                return Bytes.toBytes((String)obj);

            case ObjectType:
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(obj);
                oos.flush();
                try {
                    return baos.toByteArray();
                }
                finally {
                    oos.close();
                }
        }

        throw new HPersistException("Error in getScalarfromBytes()");
    }

    @Override
    public Object getArrayFromBytes(final FieldType fieldType, final Class clazz, final byte[] b) throws IOException, HPersistException {

        try {

            switch (fieldType) {

                case BooleanType: {
                    final ByteArrayInputStream bais = new ByteArrayInputStream(b);
                    final ObjectInputStream ois = new ObjectInputStream(bais);
                    final int length = ois.readInt();
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, ois.readBoolean());
                    }
                    return array;
                }

                case ByteType: {
                    final ByteArrayInputStream bais = new ByteArrayInputStream(b);
                    final ObjectInputStream ois = new ObjectInputStream(bais);
                    final int length = ois.readInt();
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, ois.readByte());
                    }
                    return array;
                }

                case CharType: {
                    final ByteArrayInputStream bais = new ByteArrayInputStream(b);
                    final ObjectInputStream ois = new ObjectInputStream(bais);
                    final int length = ois.readInt();
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, ois.readByte());
                        return array;
                    }
                }

                case ShortType: {
                    final ByteArrayInputStream bais = new ByteArrayInputStream(b);
                    final ObjectInputStream ois = new ObjectInputStream(bais);
                    final int length = ois.readInt();
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, ois.readShort());
                    }
                    return array;
                }

                case IntegerType: {
                    final int length = Bytes.toInt(b);
                    int offset = Bytes.SIZEOF_INT;
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, Bytes.toInt(b, offset));
                        offset += Bytes.SIZEOF_INT;
                    }
                    return array;
                }

                case LongType: {
                    final int length = Bytes.toInt(b);
                    int offset = lengthsize;
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, Bytes.toLong(b, offset));
                        offset += Bytes.SIZEOF_LONG;
                    }
                    return array;
                }

                case FloatType: {
                    final int length = Bytes.toInt(b);
                    int offset = lengthsize;
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, Bytes.toFloat(b, offset));
                        offset += Bytes.SIZEOF_FLOAT;
                    }
                    return array;
                }

                case DoubleType: {
                    final int length = Bytes.toInt(b);
                    int offset = lengthsize;
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, Bytes.toDouble(b, offset));
                        offset += Bytes.SIZEOF_DOUBLE;
                    }
                    return array;
                }

                case StringType: {
                    final ByteArrayInputStream bais = new ByteArrayInputStream(b);
                    final ObjectInputStream ois = new ObjectInputStream(bais);
                    final int length = ois.readInt();
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, ois.readUTF());
                    }
                    return array;
                }

                case ObjectType: {
                    final ByteArrayInputStream bais = new ByteArrayInputStream(b);
                    final ObjectInputStream ois = new ObjectInputStream(bais);
                    try {
                        final int length = ois.readInt();
                        final Object array = Array.newInstance(clazz, length);
                        for (int i = 0; i < length; i++) {
                            Array.set(array, i, ois.readObject());
                        }
                        return array;
                    }
                    finally {
                        ois.close();
                    }
                }
            }
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new HPersistException("Error in getScalarfromBytes()");
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
                for (final boolean val : (boolean[])obj) {
                    oos.writeBoolean(val);
                }
                break;
            }

            case ByteType: {
                oos.writeInt(((byte[])obj).length);
                for (final byte val : (byte[])obj) {
                    oos.write(val);
                }
                break;
            }

            case CharType: {
                oos.writeInt(((char[])obj).length);
                for (final char val : (char[])obj) {
                    oos.write(val);
                }
                break;
            }

            case ShortType: {
                oos.writeInt(((short[])obj).length);
                for (final short val : (short[])obj) {
                    oos.writeShort(val);
                }
                break;
            }

            case IntegerType: {
                final int arraysize = Bytes.SIZEOF_INT;
                final int length = ((int[])obj).length;
                final byte[] b = new byte[(length * arraysize) + lengthsize];
                int offset = 0;
                Bytes.putInt(b, offset, length);
                for (final int val : (int[])obj) {
                    offset += arraysize;
                    Bytes.putInt(b, offset, val);
                }
                return b;
            }

            case LongType: {
                final int arraysize = Bytes.SIZEOF_LONG;
                final int length = ((long[])obj).length;
                final byte[] b = new byte[(length * arraysize) + lengthsize];
                Bytes.putInt(b, 0, length);
                int offset = lengthsize;
                for (final long val : (long[])obj) {
                    Bytes.putLong(b, offset, val);
                    offset += arraysize;
                }
                return b;
            }

            case FloatType: {
                final int arraysize = Bytes.SIZEOF_FLOAT;
                final int length = ((float[])obj).length;
                final byte[] b = new byte[(length * arraysize) + lengthsize];
                Bytes.putInt(b, 0, length);
                int offset = lengthsize;
                for (final float val : (float[])obj) {
                    Bytes.putFloat(b, offset, val);
                    offset += arraysize;
                }
                return b;
            }

            case DoubleType: {
                final int arraysize = Bytes.SIZEOF_DOUBLE;
                final int length = ((double[])obj).length;
                final byte[] b = new byte[(length * arraysize) + lengthsize];
                Bytes.putInt(b, 0, length);
                int offset = lengthsize;
                for (final double val : (double[])obj) {
                    Bytes.putDouble(b, offset, val);
                    offset += arraysize;
                }
                return b;
            }

            case StringType: {
                oos.writeInt(((Object[])obj).length);
                for (final String val : (String[])obj) {
                    oos.writeUTF(val);
                }
                break;
            }

            case ObjectType: {
                oos.writeInt(((Object[])obj).length);
                for (final Object val : (Object[])obj) {
                    oos.writeObject(val);
                }
                break;
            }
        }
        oos.flush();
        return baos.toByteArray();
    }

}