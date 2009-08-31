package com.imap4j.hbase.hbql.io;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.schema.ClassSchema;
import com.imap4j.hbase.hbql.schema.FieldType;
import org.apache.hadoop.hbase.client.Result;

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

    public static Serialization newSerializationStrategy(final TYPE type) {

        switch (type) {
            case JAVA:
                return new JavaSerialization();
            // case HADOOP:
            //     return new HadoopSerialization();
        }

        return null;
    }

    public boolean isSerializable(final Object obj) {

        try {
            final byte[] b = getObjectAsBytes(obj);
            Object newobj = getObjectFromBytes(b);
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    abstract public byte[] getObjectAsBytes(Object obj) throws IOException;

    abstract public Object getObjectFromBytes(byte[] b) throws IOException, HPersistException;

    abstract public Object getScalarFromBytes(FieldType fieldType, byte[] b) throws IOException, HPersistException;

    abstract public byte[] getScalarAsBytes(FieldType fieldType, Object obj) throws IOException, HPersistException;

    abstract public Object getArrayFromBytes(FieldType fieldType, Class clazz, byte[] b) throws IOException, HPersistException;

    abstract public byte[] getArrayasBytes(FieldType fieldType, Object obj) throws IOException, HPersistException;

    abstract public HPersistable getHPersistable(ClassSchema classSchema, Result result) throws HPersistException;
}
