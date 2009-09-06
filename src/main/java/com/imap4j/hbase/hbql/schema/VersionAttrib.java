package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HColumnVersionMap;
import com.imap4j.hbase.hbql.HPersistException;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 5, 2009
 * Time: 10:03:49 PM
 */
public class VersionAttrib implements Serializable {

    private final Field field;

    public VersionAttrib(final ClassSchema classSchamea, final Field field) throws HPersistException {
        this.field = field;

        this.verify(classSchamea);

    }

    private Field getField() {
        return field;
    }

    private HColumnVersionMap getColumnVersionMapAnnotation() {
        return this.getField().getAnnotation(HColumnVersionMap.class);
    }

    public String getVariableName() {
        return this.getField().getName();
    }

    public String getObjectQualifiedName() {
        return this.getEnclosingClass().getName() + "." + this.getVariableName();
    }

    private Class getEnclosingClass() {
        return this.getField().getDeclaringClass();
    }

    public String getInstanceVariableName() {
        return this.getColumnVersionMapAnnotation().instance();
    }

    public void verify(final ClassSchema classSchema) throws HPersistException {

        // Check if instance variable exists
        if (!classSchema.constainsVariableName(this.getInstanceVariableName())) {
            throw new HPersistException("@HColumnVersionMap annotation for " + this.getObjectQualifiedName()
                                        + " refers to invalid instance variable " + this.getInstanceVariableName());
        }

        // Check if it is a Map
        if (!Map.class.isAssignableFrom(field.getType()))
            throw new HPersistException(this.getObjectQualifiedName() + "has a @HColumnVersionMap annotation so it "
                                        + "must implement the Map interface");
    }
}
