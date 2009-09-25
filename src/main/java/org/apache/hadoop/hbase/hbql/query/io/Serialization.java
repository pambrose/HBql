package org.apache.hadoop.hbase.hbql.query.io;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 3:55:02 PM
 */
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

    abstract public Object getScalarFromBytes(FieldType fieldType, byte[] b) throws IOException, HBqlException;

    abstract public byte[] getScalarAsBytes(FieldType fieldType, Object obj) throws IOException, HBqlException;

    abstract public Object getArrayFromBytes(FieldType fieldType, Class clazz, byte[] b) throws IOException, HBqlException;

    abstract public byte[] getArrayasBytes(FieldType fieldType, Object obj) throws IOException, HBqlException;

    public byte[] getStringAsBytes(final String obj) throws IOException, HBqlException {
        return this.getScalarAsBytes(FieldType.StringType, obj);
    }

    public byte[] getObjectAsBytes(final Object obj) throws IOException, HBqlException {
        return this.getScalarAsBytes(FieldType.getFieldType(obj), obj);
    }

    public String getStringFromBytes(final byte[] b) throws IOException, HBqlException {
        return (String)this.getScalarFromBytes(FieldType.StringType, b);
    }

    public Object getObjectFromBytes(final FieldType type, final byte[] b) throws IOException, HBqlException {
        return this.getScalarFromBytes(type, b);
    }

    public boolean isSerializable(final Object obj) {

        try {
            final byte[] b = getObjectAsBytes(obj);
            final Object newobj = getObjectFromBytes(FieldType.getFieldType(obj), b);
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
