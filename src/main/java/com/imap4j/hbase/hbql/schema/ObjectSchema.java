package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HPersistException;

import java.lang.reflect.Field;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 9, 2009
 * Time: 10:19:00 AM
 */
public class ObjectSchema extends ExprSchema {

    public ObjectSchema(final Object object) throws HPersistException {

        for (final Field field : object.getClass().getDeclaredFields()) {
            if (field.getType().isPrimitive() && !field.getType().isArray()) {
                final ReflectionAttrib attrib = new ReflectionAttrib(field);
                addVariableAttrib(attrib);
            }
        }
    }
}