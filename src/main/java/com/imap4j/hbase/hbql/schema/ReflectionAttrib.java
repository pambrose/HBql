package com.imap4j.hbase.hbql.schema;

import java.lang.reflect.Field;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:19:35 PM
 */
public class ReflectionAttrib extends FieldAttrib {

    public ReflectionAttrib(final Field field) {
        super(FieldType.getFieldType(field.getType()), field);
    }
}