package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistException;

import java.lang.reflect.Field;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 5:01:22 PM
 */
public enum ExprType {

    BooleanType(Boolean.TYPE),
    ByteType(Byte.TYPE),
    CharType(Character.TYPE),
    ShortType(Short.TYPE),
    IntegerType(Integer.TYPE),
    LongType(Long.TYPE),
    FloatType(Float.TYPE),
    DoubleType(Double.TYPE),
    NumberType(Number.class),
    StringType(String.class),
    DateType(Date.class),
    ObjectType(Object.class);

    private final Class clazz;

    ExprType(final Class clazz) {
        this.clazz = clazz;
    }

    private Class getClazz() {
        return clazz;
    }

    static ExprType getExprType(final Field field) throws HPersistException {

        final Class fieldClass = field.getType();

        final Class<?> clazz = fieldClass.isArray() ? fieldClass.getComponentType() : fieldClass;

        if (!clazz.isPrimitive()) {
            return ObjectType;
        }
        else {
            for (final ExprType type : values())
                if (clazz == type.getClazz())
                    return type;
        }

        throw new HPersistException("Not able to deal with type: " + clazz);
    }
}