package org.apache.hadoop.hbase.contrib.hbql.schema;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.contrib.hbql.client.Connection;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class ReflectionSchema extends Schema {

    private final static Map<Class<?>, ReflectionSchema> reflectionSchemaMap = Maps.newHashMap();

    private ReflectionSchema(final Class clazz) throws HBqlException {
        super(clazz.getName());

        for (final Field field : clazz.getDeclaredFields()) {

            if (field.getType().isArray())
                continue;

            if (field.getType().isPrimitive()
                || field.getType().equals(String.class)
                || field.getType().equals(Date.class)) {
                final ReflectionAttrib attrib = new ReflectionAttrib(field);
                addAttribToVariableNameMap(attrib, attrib.getVariableName());
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
        return reflectionSchemaMap;
    }

    public Collection<String> getSchemaFamilyNames(final Connection connection) throws HBqlException {
        return Lists.newArrayList();
    }
}