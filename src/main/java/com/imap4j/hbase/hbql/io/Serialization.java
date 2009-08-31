package com.imap4j.hbase.hbql.io;

import com.google.common.collect.Maps;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.schema.ClassSchema;
import com.imap4j.hbase.hbql.schema.FieldAttrib;
import com.imap4j.hbase.hbql.schema.FieldType;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Map;

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
            case HADOOP:
                return new HadoopSerialization();
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

    public HPersistable getHPersistable(final ClassSchema classSchema, final Result result) throws HPersistException {

        try {
            final HPersistable newobj = (HPersistable)classSchema.getClazz().newInstance();
            final FieldAttrib keyattrib = classSchema.getKeyFieldAttrib();

            final byte[] keybytes = result.getRow();
            final Object keyval = keyattrib.getValueFromBytes(this, newobj, keybytes);
            classSchema.getKeyFieldAttrib().getField().set(newobj, keyval);

            for (final KeyValue keyValue : result.list()) {

                final byte[] colbytes = keyValue.getColumn();
                final String column = new String(colbytes);
                final byte[] valbytes = result.getValue(colbytes);

                if (column.endsWith("]")) {
                    final int lbrace = column.indexOf("[");
                    final String mapcolumn = column.substring(0, lbrace);
                    final String mapKey = column.substring(lbrace + 1, column.length() - 1);
                    final FieldAttrib attrib = classSchema.getFieldAttribMapByColumn().get(mapcolumn);
                    final Object val = attrib.getValueFromBytes(this, newobj, valbytes);
                    Map mapval = (Map)attrib.getValue(newobj);

                    // TODO Not sure if it is kosher to create a map here
                    if (mapval == null) {
                        mapval = Maps.newHashMap();
                        attrib.getField().set(newobj, mapval);
                    }

                    mapval.put(mapKey, val);
                }
                else {
                    final FieldAttrib attrib = classSchema.getFieldAttribMapByColumn().get(column);
                    final Object val = attrib.getValueFromBytes(this, newobj, valbytes);
                    attrib.getField().set(newobj, val);
                }
            }

            return newobj;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new HPersistException("Error in execute()");
        }
    }

}
