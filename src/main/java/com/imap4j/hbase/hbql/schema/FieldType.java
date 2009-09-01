package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HPersistException;

import java.lang.reflect.Field;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 5:01:22 PM
 */
public enum FieldType {

    BooleanType(Boolean.TYPE),
    ByteType(Byte.TYPE),
    CharType(Short.TYPE),
    ShortType(Short.TYPE),
    IntegerType(Integer.TYPE),
    LongType(Long.TYPE),
    FloatType(Float.TYPE),
    DoubleType(Double.TYPE),
    StringType(String.class),
    ObjectType(Object.class);

    private final Class clazz;

    FieldType(final Class clazz) {
        this.clazz = clazz;
    }

    private Class getClazz() {
        return clazz;
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
