package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 9, 2009
 * Time: 10:19:00 AM
 */
public class ReflectionSchema extends Schema {

    private final static Map<Class<?>, ReflectionSchema> ReflectionSchemaMap = Maps.newHashMap();

    private final Class<?> clazz;

    private ReflectionSchema(final Class clazz) throws HBqlException {
        this.clazz = clazz;

        for (final Field field : clazz.getDeclaredFields()) {

            if (field.getType().isArray())
                continue;

            if (field.getType().isPrimitive()
                || field.getType().equals(String.class)
                || field.getType().equals(Date.class)) {
                final ReflectionAttrib attrib = new ReflectionAttrib(field);
                addAttribToVariableNameMap(attrib);
            }
        }
    }

    public static ReflectionSchema getReflectionSchema(final Object obj) throws HBqlException {
        return getReflectionSchema(obj.getClass());
    }

    public synchronized static ReflectionSchema getReflectionSchema(final Class clazz) throws HBqlException {

        ReflectionSchema schema = getReflectionSchemaMap().get(clazz);
        if (schema != null)
            return schema;

        schema = new ReflectionSchema(clazz);
        getReflectionSchemaMap().put(clazz, schema);
        return schema;
    }

    private static Map<Class<?>, ReflectionSchema> getReflectionSchemaMap() {
        return ReflectionSchemaMap;
    }
}