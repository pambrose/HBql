package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HPersistException;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 5:01:22 PM
 */
public enum FieldType {

    BooleanType(Boolean.TYPE, Bytes.SIZEOF_BOOLEAN),
    ByteType(Byte.TYPE, Bytes.SIZEOF_BYTE),
    CharType(Short.TYPE, Bytes.SIZEOF_CHAR),
    ShortType(Short.TYPE, Bytes.SIZEOF_SHORT),
    IntegerType(Integer.TYPE, Bytes.SIZEOF_INT),
    LongType(Long.TYPE, Bytes.SIZEOF_LONG),
    FloatType(Float.TYPE, Bytes.SIZEOF_FLOAT),
    DoubleType(Double.TYPE, Bytes.SIZEOF_DOUBLE),
    StringType(String.class, -1),
    DateType(Date.class, -1),
    ObjectType(Object.class, -1);

    private final Class clazz;
    private final int size;


    FieldType(final Class clazz, final int size) {
        this.clazz = clazz;
        this.size = size;
    }

    private Class getClazz() {
        return clazz;
    }

    public int getSize() {
        return size;
    }

    public static FieldType getFieldType(final Object obj) throws HPersistException {
        final Class fieldClass = obj.getClass();
        return getFieldType(fieldClass);
    }

    public static FieldType getFieldType(final Field field) throws HPersistException {
        final Class fieldClass = field.getType();
        return getFieldType(fieldClass);
    }

    public static FieldType getFieldType(final Class fieldClass) throws HPersistException {

        final Class<?> clazz = fieldClass.isArray() ? fieldClass.getComponentType() : fieldClass;

        if (!clazz.isPrimitive()) {
            if (clazz.equals(String.class))
                return StringType;
            else if (clazz.equals(Date.class))
                return DateType;
            else
                return ObjectType;
        }
        else {
            for (final FieldType type : values())
                if (clazz == type.getClazz())
                    return type;
        }

        throw new HPersistException("Not able to deal with type: " + clazz);
    }
}
