package com.imap4j.hbase.hbql;

import java.lang.reflect.Field;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 5:01:22 PM
 */
enum FieldType {

    BooleanType(Boolean.TYPE),
    ByteType(Byte.TYPE),
    CharType(Character.TYPE),
    ShortType(Short.TYPE),
    IntegerType(Integer.TYPE),
    LongType(Long.TYPE),
    FloatType(Float.TYPE),
    DoubleType(Double.TYPE),
    EnumType(Enum.class),
    ObjectType(Object.class);

    private final Class clazz;

    FieldType(final Class clazz) {
        this.clazz = clazz;
    }

    private Class getClazz() {
        return clazz;
    }

    static FieldType getFieldType(final Field field) throws HBPersistException {

        final Class fieldClass = field.getType();

        final Class<?> clazz = fieldClass.isArray() ? fieldClass.getComponentType() : fieldClass;

        if (!clazz.isPrimitive()) {
            return ObjectType;
        }
        else {
            for (final FieldType type : values())
                if (clazz == type.getClazz())
                    return type;
        }

        throw new HBPersistException("Not able to deal with type: " + clazz);
    }
}
