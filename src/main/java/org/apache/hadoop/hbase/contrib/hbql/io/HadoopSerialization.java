package org.apache.hadoop.hbase.contrib.hbql.io;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.schema.FieldType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;

public class HadoopSerialization extends Serialization {

    private static final int arraysize = Bytes.SIZEOF_INT;

    public Object getScalarFromBytes(final FieldType fieldType, final byte[] b) throws HBqlException {

        if (b == null)
            return null;

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

                case KeyType:
                case StringType:
                    return Bytes.toString(b);

                case DateType:
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

                default:
                    throw new HBqlException("Unknown type in getScalarfromBytes() - " + fieldType);
            }
        }
        catch (IOException e) {
            throw new HBqlException("Error in getScalarfromBytes() " + e.getMessage());
        }
        catch (ClassNotFoundException e) {
            throw new HBqlException("Error in getScalarfromBytes() " + e.getMessage());
        }
    }

    public byte[] getScalarAsBytes(final FieldType fieldType, final Object obj) throws HBqlException {

        if (obj == null)
            return null;

        try {
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

                case KeyType:
                case StringType:
                    return Bytes.toBytes((String)obj);

                case DateType:
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

                default:
                    throw new HBqlException("Unknown type in getScalarfromBytes() - " + fieldType);
            }
        }
        catch (IOException e) {
            throw new HBqlException("Error in getScalarfromBytes() - " + e.getMessage());
        }
    }

    public Object getArrayFromBytes(final FieldType fieldType, final Class clazz, final byte[] b) throws HBqlException {

        if (b == null)
            return null;

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
                    final int length = this.readLength(b);
                    int offset = arraysize;
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, b[offset]);
                        offset += fieldType.getSize();
                    }
                    return array;
                }

                case CharType: {
                    final String s = new String(b);
                    return s.toCharArray();
                }

                case ShortType: {
                    final int length = this.readLength(b);
                    int offset = arraysize;
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, Bytes.toShort(b, offset));
                        offset += fieldType.getSize();
                    }
                    return array;
                }

                case IntegerType: {
                    final int length = this.readLength(b);
                    int offset = arraysize;
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, Bytes.toInt(b, offset));
                        offset += fieldType.getSize();
                    }
                    return array;
                }

                case LongType: {
                    final int length = this.readLength(b);
                    int offset = arraysize;
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        Array.set(array, i, Bytes.toLong(b, offset));
                        offset += fieldType.getSize();
                    }
                    return array;
                }

                case FloatType: {
                    final int length = this.readLength(b);
                    int offset = arraysize;
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        final float val = Bytes.toFloat(b, offset);
                        Array.set(array, i, val);
                        offset += fieldType.getSize();
                    }
                    return array;
                }

                case DoubleType: {
                    final int length = this.readLength(b);
                    int offset = arraysize;
                    final Object array = Array.newInstance(clazz, length);
                    for (int i = 0; i < length; i++) {
                        final double val = Bytes.toDouble(b, offset);
                        Array.set(array, i, val);
                        offset += fieldType.getSize();
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

                case DateType:
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

                default:
                    throw new HBqlException("Unknown type in getScalarfromBytes() - " + fieldType);
            }
        }
        catch (IOException e) {
            throw new HBqlException("Error in getScalarfromBytes() " + e.getMessage());
        }
        catch (ClassNotFoundException e) {
            throw new HBqlException("Error in getScalarfromBytes() " + e.getMessage());
        }
    }

    public byte[] getArrayasBytes(final FieldType fieldType, final Object obj) throws HBqlException {

        if (obj == null)
            return null;

        try {
            switch (fieldType) {

                case BooleanType: {
                    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    final ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeInt(((boolean[])obj).length);
                    for (final boolean val : (boolean[])obj) {
                        oos.writeBoolean(val);
                    }
                    oos.flush();
                    return baos.toByteArray();
                }

                case ByteType: {
                    final int length = ((byte[])obj).length;
                    final byte[] b = new byte[(length * fieldType.getSize()) + arraysize];
                    this.writeLength(b, length);
                    int offset = arraysize;
                    for (final byte val : (byte[])obj) {
                        Bytes.putByte(b, offset, val);
                        offset += fieldType.getSize();
                    }
                    return b;
                }

                case CharType: {
                    final String s = new String((char[])obj);
                    return Bytes.toBytes(s);
                }

                case ShortType: {
                    final int length = ((short[])obj).length;
                    final byte[] b = new byte[(length * fieldType.getSize()) + arraysize];
                    this.writeLength(b, length);
                    int offset = arraysize;
                    for (final short val : (short[])obj) {
                        Bytes.putShort(b, offset, val);
                        offset += fieldType.getSize();
                    }
                    return b;
                }

                case IntegerType: {
                    final int length = ((int[])obj).length;
                    final byte[] b = new byte[(length * fieldType.getSize()) + arraysize];
                    this.writeLength(b, length);
                    int offset = arraysize;
                    for (final int val : (int[])obj) {
                        Bytes.putInt(b, offset, val);
                        offset += fieldType.getSize();
                    }
                    return b;
                }

                case LongType: {
                    final int length = ((long[])obj).length;
                    final byte[] b = new byte[(length * fieldType.getSize()) + arraysize];
                    this.writeLength(b, length);
                    int offset = arraysize;
                    for (final long val : (long[])obj) {
                        Bytes.putLong(b, offset, val);
                        offset += fieldType.getSize();
                    }
                    return b;
                }

                case FloatType: {
                    final int length = ((float[])obj).length;
                    final byte[] b = new byte[(length * fieldType.getSize()) + arraysize];
                    this.writeLength(b, length);
                    int offset = arraysize;
                    for (final float val : (float[])obj) {
                        Bytes.putFloat(b, offset, val);
                        offset += fieldType.getSize();
                    }
                    return b;
                }

                case DoubleType: {
                    final int length = ((double[])obj).length;
                    final byte[] b = new byte[(length * fieldType.getSize()) + arraysize];
                    this.writeLength(b, length);
                    int offset = arraysize;
                    for (final double val : (double[])obj) {
                        Bytes.putDouble(b, offset, val);
                        offset += fieldType.getSize();
                    }
                    return b;
                }

                case StringType: {
                    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    final ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeInt(((Object[])obj).length);
                    for (final String val : (String[])obj) {
                        oos.writeUTF(val);
                    }
                    oos.flush();
                    return baos.toByteArray();
                }

                case DateType:
                case ObjectType: {
                    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    final ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeInt(((Object[])obj).length);
                    for (final Object val : (Object[])obj) {
                        oos.writeObject(val);
                    }
                    oos.flush();
                    return baos.toByteArray();
                }

                default:
                    throw new HBqlException("Unknown type in getArrayasBytes() - " + fieldType);
            }
        }
        catch (IOException e) {
            throw new HBqlException("Error in getArrayasBytes() - " + e.getMessage());
        }
    }

    private int readLength(final byte[] b) {
        return Bytes.toInt(b);
    }

    private void writeLength(final byte[] b, final int length) {
        Bytes.putInt(b, 0, length);
    }
}