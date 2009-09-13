package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.util.Maps;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 9, 2009
 * Time: 10:19:00 AM
 */
public class ObjectSchema extends ExprSchema {

    private final static Map<Class<?>, ObjectSchema> objectSchemaMap = Maps.newHashMap();

    private final Class<?> clazz;

    private ObjectSchema(final Class clazz) throws HPersistException {
        this.clazz = clazz;

        for (final Field field : clazz.getDeclaredFields()) {

            if (field.getType().isArray())
                continue;

            if (field.getType().isPrimitive()
                || field.getType().equals(String.class)
                || field.getType().equals(Date.class)) {
                final ReflectionAttrib attrib = new ReflectionAttrib(field);
                addVariableAttrib(attrib);
            }
        }
    }

    public static ObjectSchema getObjectSchema(final Object obj) throws HPersistException {
        return getObjectSchema(obj.getClass());
    }

    public synchronized static ObjectSchema getObjectSchema(final Class clazz) throws HPersistException {

        ObjectSchema schema = getObjectSchemaMap().get(clazz);
        if (schema != null)
            return schema;

        schema = new ObjectSchema(clazz);
        getObjectSchemaMap().put(clazz, schema);
        return schema;
    }

    private static Map<Class<?>, ObjectSchema> getObjectSchemaMap() {
        return objectSchemaMap;
    }

    private Class<?> getClazz() {
        return clazz;
    }

    /*
    public String getSchemaName() {
        return this.getTableName();
    }

    public String getTableName() {
        return this.getClazz().getName();
    }
    */
}