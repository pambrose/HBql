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

    public VersionAttrib(final ClassSchema classSchamea, final Field field) throws HPersistException {
        super(field,
              field.getAnnotation(HColumnVersionMap.class).family(),
              field.getAnnotation(HColumnVersionMap.class).column(),
              field.getAnnotation(HColumnVersionMap.class).getter(),
              field.getAnnotation(HColumnVersionMap.class).setter(),
              field.getAnnotation(HColumnVersionMap.class).mapKeysAsColumns());

        // Now check relative to instance variable
        this.verify(classSchamea);
    }

    private HColumnVersionMap getColumnVersionMapAnnotation() {
        return this.getField().getAnnotation(HColumnVersionMap.class);
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
        if (!Map.class.isAssignableFrom(this.getField().getType()))
            throw new HPersistException(this.getObjectQualifiedName() + "has a @HColumnVersionMap annotation so it "
                                        + "must implement the Map interface");
    }
}
