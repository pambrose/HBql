package org.apache.hadoop.hbase.hbql.query.io;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.stmt.schema.FieldType;

public abstract class Serialization {

    public enum TYPE {
        JAVA, HADOOP
    }

    private final static Serialization java = new JavaSerialization();
    private final static Serialization hadoop = new HadoopSerialization();

    public static Serialization getSerializationStrategy(final TYPE type) {

        switch (type) {
            case JAVA:
                return java;
            case HADOOP:
                return hadoop;
        }

        return null;
    }

    abstract public Object getScalarFromBytes(FieldType fieldType, byte[] b) throws HBqlException;

    abstract public byte[] getScalarAsBytes(FieldType fieldType, Object obj) throws HBqlException;

    abstract public Object getArrayFromBytes(FieldType fieldType, Class clazz, byte[] b) throws HBqlException;

    abstract public byte[] getArrayasBytes(FieldType fieldType, Object obj) throws HBqlException;

    public byte[] getStringAsBytes(final String obj) throws HBqlException {
        return this.getScalarAsBytes(FieldType.StringType, obj);
    }

    public byte[] getScalarAsBytes(final Object obj) throws HBqlException {
        return this.getScalarAsBytes(FieldType.getFieldType(obj), obj);
    }

    public String getStringFromBytes(final byte[] b) throws HBqlException {
        return (String)this.getScalarFromBytes(FieldType.StringType, b);
    }

    public boolean isSerializable(final Object obj) {

        try {
            final byte[] b = getScalarAsBytes(obj);
            final Object newobj = getScalarFromBytes(FieldType.getFieldType(obj), b);
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
