package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HColumnVersionMap;
import com.imap4j.hbase.hbql.HPersistException;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 5, 2009
 * Time: 10:03:49 PM
 */
public class VersionAttrib extends ColumnAttrib {

    private VersionAttrib(final ClassSchema classSchamea, final Field field) throws HPersistException {
        super(field,
              FieldType.getFieldType(field),
              field.getAnnotation(HColumnVersionMap.class).family(),
              field.getAnnotation(HColumnVersionMap.class).column(),
              field.getAnnotation(HColumnVersionMap.class).getter(),
              field.getAnnotation(HColumnVersionMap.class).setter(),
              field.getAnnotation(HColumnVersionMap.class).mapKeysAsColumns());

    }

    public static VersionAttrib createVersionAttrib(final ClassSchema classSchema, final Field field) throws HPersistException {

        final HColumnVersionMap anno = field.getAnnotation(HColumnVersionMap.class);
        final String instance = anno.instance();

        if (instance.length() > 0) {
            if (anno.family().length() > 0)
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and family value in annotation");
            if (anno.column().length() > 0)
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and column value in annotation");
            if (anno.getter().length() > 0)
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and getter value in annotation");
            if (anno.setter().length() > 0)
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and setter value in annotation");
            // This doesn't test false values -- they wil be ignored
            if (anno.mapKeysAsColumns())
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and mapKeysAsColumns value in annotation");

            // Check if instance variable exists
            if (!classSchema.constainsVariableName(instance)) {
                throw new HPersistException("@HColumnVersionMap annotation for " + getObjectQualifiedName(field)
                                            + " refers to invalid instance variable " + instance);
            }

            // Check if it is a Map
            if (!Map.class.isAssignableFrom(field.getType()))
                throw new HPersistException(getObjectQualifiedName(field) + "has a @HColumnVersionMap annotation so it "
                                            + "must implement the Map interface");

        }
        else {

        }

        return new VersionAttrib(classSchema, field);
    }


    private HColumnVersionMap getColumnVersionMapAnnotation() {
        return this.getField().getAnnotation(HColumnVersionMap.class);
    }

    public String getInstanceVariableName() {
        return this.getColumnVersionMapAnnotation().instance();
    }

}
