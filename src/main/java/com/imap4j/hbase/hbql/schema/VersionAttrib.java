package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HColumn;
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

    private VersionAttrib(final Field field,
                          final FieldType fieldType,
                          final String family,
                          final String column,
                          final String getter,
                          final String setter,
                          final boolean mapKeysAsColumns) throws HPersistException {
        super(field, fieldType, family, column, getter, setter, mapKeysAsColumns);
    }

    public static VersionAttrib createVersionAttrib(final ClassSchema classSchema, final Field field) throws HPersistException {

        final HColumnVersionMap versionAnno = field.getAnnotation(HColumnVersionMap.class);
        final String instance = versionAnno.instance();

        if (instance.length() > 0) {
            if (versionAnno.family().length() > 0)
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and family value in annotation");
            if (versionAnno.column().length() > 0)
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and column value in annotation");
            if (versionAnno.getter().length() > 0)
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and getter value in annotation");
            if (versionAnno.setter().length() > 0)
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and setter value in annotation");
            // This doesn't test false values -- they wil be ignored
            if (versionAnno.mapKeysAsColumns())
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

            final ColumnAttrib columnAttrib = (ColumnAttrib)classSchema.getVariableAttribByVariableName(instance);

            if (!columnAttrib.isCurrentValueAttrib())
                throw new HPersistException(getObjectQualifiedName(field) + "instance variable must have HColumn annotation");

            final CurrentValueAttrib currentAttrib = (CurrentValueAttrib)columnAttrib;

            final HColumn columnAnno = currentAttrib.getColumnAnno();

            return new VersionAttrib(field,
                                     currentAttrib.getFieldType(),
                                     columnAnno.family(),
                                     columnAnno.column(),
                                     columnAnno.getter(),
                                     columnAnno.setter(),
                                     columnAnno.mapKeysAsColumns());
        }
        else {

            return new VersionAttrib(field,
                                     FieldType.getFieldType(field),
                                     versionAnno.family(),
                                     versionAnno.column(),
                                     versionAnno.getter(),
                                     versionAnno.setter(),
                                     versionAnno.mapKeysAsColumns());

        }

    }

    @Override
    public boolean isCurrentValueAttrib() {
        return false;
    }

}
