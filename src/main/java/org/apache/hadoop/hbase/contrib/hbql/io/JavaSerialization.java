package org.apache.hadoop.hbase.contrib.hbql.io;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.schema.FieldType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;

public class JavaSerialization extends Serialization {

    public Object getScalarFromBytes(final FieldType fieldType, final byte[] b) throws HBqlException {

        if (b == null)
            return null;

        ObjectInputStream ois = null;

        try {

            final ByteArrayInputStream bais = new ByteArrayInputStream(b);
            ois = new ObjectInputStream(bais);

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

                case KeyType:
                case StringType:
                    return ois.readUTF();

                case DateType:
                case ObjectType:
                    return ois.readObject();

                default:
                    throw new HBqlException("Unknown type in getScalarfromBytes() " + fieldType);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new HBqlException("Error in getScalarfromBytes() " + e.getMessage());
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new HBqlException("Error in getScalarfromBytes() " + e.getMessage());
        }
        finally {
            if (ois != null) {
                try {
                    ois.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public byte[] getScalarAsBytes(final FieldType fieldType, final Object obj) throws HBqlException {

        if (obj == null)
            return null;

        try {
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

                case KeyType:
                case StringType:
                    oos.writeUTF((String)obj);
                    break;

                case DateType:
                case ObjectType:
                    oos.writeObject(obj);
                    break;

                default:
                    throw new HBqlException("Unknown type in getScalarAsBytes() - " + fieldType);
            }

            oos.flush();
            return baos.toByteArray();
        }
        catch (IOException e) {
            throw new HBqlException("Error in getScalarAsBytes() - " + fieldType);
        }
    }

    public Object getArrayFromBytes(final FieldType fieldType, final Class clazz, final byte[] b) throws HBqlException {

        if (b == null)
            return null;

        if (fieldType == FieldType.CharType) {
            final String s = new String(b);
            return s.toCharArray();
        }

        ObjectInputStream ois = null;

        try {
            final ByteArrayInputStream bais = new ByteArrayInputStream(b);
            ois = new ObjectInputStream(bais);

            // Read length
            final int length = ois.readInt();
            final Object array = Array.newInstance(clazz, length);

            switch (fieldType) {

                case BooleanType:
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readBoolean());
                    return array;

                case ByteType:
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readByte());
                    return array;

                case CharType:
                    // See above
                    return null;

                case ShortType:
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readShort());
                    return array;

                case IntegerType:
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readInt());
                    return array;

                case LongType:
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readLong());
                    return array;

                case FloatType:
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readFloat());
                    return array;

                case DoubleType:
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readDouble());
                    return array;

                case StringType:
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readUTF());
                    return array;

                case DateType:
                case ObjectType:
                    for (int i = 0; i < length; i++)
                        Array.set(array, i, ois.readObject());
                    return array;

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
        finally {
            if (ois != null) {
                try {
                    ois.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public byte[] getArrayasBytes(final FieldType fieldType, final Object obj) throws HBqlException {

        if (obj == null)
            return null;

        if (fieldType == FieldType.CharType) {
            final String s = new String((char[])obj);
            return s.getBytes();
        }

        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final ObjectOutputStream oos = new ObjectOutputStream(baos);

            switch (fieldType) {

                case BooleanType:
                    oos.writeInt(((boolean[])obj).length);
                    for (final boolean val : (boolean[])obj)
                        oos.writeBoolean(val);
                    break;

                case ByteType:
                    oos.writeInt(((byte[])obj).length);
                    for (final byte val : (byte[])obj)
                        oos.write(val);
                    break;

                case CharType:
                    // See above
                    break;

                case ShortType:
                    oos.writeInt(((short[])obj).length);
                    for (final short val : (short[])obj)
                        oos.writeShort(val);
                    break;

                case IntegerType:
                    oos.writeInt(((int[])obj).length);
                    for (final int val : (int[])obj)
                        oos.writeInt(val);
                    break;

                case LongType:
                    oos.writeInt(((long[])obj).length);
                    for (final long val : (long[])obj)
                        oos.writeLong(val);
                    break;

                case FloatType:
                    oos.writeInt(((float[])obj).length);
                    for (final float val : (float[])obj)
                        oos.writeFloat(val);
                    break;

                case DoubleType:
                    oos.writeInt(((double[])obj).length);
                    for (final double val : (double[])obj)
                        oos.writeDouble(val);
                    break;

                case StringType:
                    oos.writeInt(((String[])obj).length);
                    for (final String val : (String[])obj)
                        oos.writeUTF(val);
                    break;

                case DateType:
                case ObjectType:
                    oos.writeInt(((Object[])obj).length);
                    for (final Object val : (Object[])obj)
                        oos.writeObject(val);
                    break;

                default:
                    throw new HBqlException("Unknown type in getArrayasBytes() - " + fieldType);
            }
            oos.flush();
            return baos.toByteArray();
        }
        catch (IOException e) {
            throw new HBqlException("Error in getArrayasBytes() " + e.getMessage());
        }
    }
}
