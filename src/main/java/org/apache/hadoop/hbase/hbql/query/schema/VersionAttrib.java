package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HColumnVersionMap;
import org.apache.hadoop.hbase.hbql.client.HPersistException;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 5, 2009
 * Time: 10:03:49 PM
 */
public class VersionAttrib extends FieldAttrib {

    private VersionAttrib(final Field field,
                          final FieldType fieldType,
                          final String family,
                          final String column,
                          final String getter,
                          final String setter,
                          final boolean mapKeysAsColumns) throws HPersistException {
        super(field, fieldType, family, column, getter, setter, mapKeysAsColumns);

        this.defineAccessors();
    }

    public static VersionAttrib newVersionAttrib(final HBaseSchema schema, final Field field) throws HPersistException {

        final HColumnVersionMap versionAnno = field.getAnnotation(HColumnVersionMap.class);
        final String instance = versionAnno.instance();

        final String annoname = "@HColumnVersionMap annotation";

        // Check if type is a Map
        if (!Map.class.isAssignableFrom(field.getType()))
            throw new HPersistException(getObjectQualifiedName(field) + "has a " + annoname + " so it "
                                        + "must implement the Map interface");

        // Look up type of map value
        final ParameterizedType ptype = (ParameterizedType)field.getGenericType();
        final Type[] typeargs = ptype.getActualTypeArguments();
        final Type mapValueType = typeargs[1];

        if (instance.length() > 0) {
            if (versionAnno.family().length() > 0)
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and family value in " + annoname);
            if (versionAnno.column().length() > 0)
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and column value in " + annoname);
            if (versionAnno.getter().length() > 0)
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and getter value in " + annoname);
            if (versionAnno.setter().length() > 0)
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and setter value in " + annoname);

            // This doesn't test false values -- they wil be ignored
            if (versionAnno.mapKeysAsColumns())
                throw new HPersistException(getObjectQualifiedName(field)
                                            + " cannot have both an instance and mapKeysAsColumns value in " + annoname);

            // Check if instance variable exists
            if (!schema.constainsVariableName(instance))
                throw new HPersistException(annoname + " for " + getObjectQualifiedName(field)
                                            + " refers to invalid instance variable " + instance);

            final ColumnAttrib attrib = (ColumnAttrib)schema.getVariableAttribByVariableName(instance);

            if (attrib == null)
                throw new HPersistException("Instance variable " + instance
                                            + " does not exist in " + schema.getTableName());

            if (!attrib.isACurrentValue())
                throw new HPersistException(getObjectQualifiedName(field)
                                            + "instance variable must have HColumn annotation");

            final CurrentValueAttrib currentAttrib = (CurrentValueAttrib)attrib;

            // Make sure type of Value in map matches type of instance var
            if (!mapValueType.equals(currentAttrib.getField().getType()))
                throw new HPersistException("Type of " + getObjectQualifiedName(field) + " map value type does not " +
                                            "match type of " + currentAttrib.getObjectQualifiedName());

            return new VersionAttrib(field,
                                     currentAttrib.getFieldType(),
                                     currentAttrib.getFamilyName(),
                                     currentAttrib.getColumnName(),
                                     currentAttrib.getGetter(),
                                     currentAttrib.getSetter(),
                                     currentAttrib.isMapKeysAsColumns());
        }
        else {
            return new VersionAttrib(field,
                                     FieldType.getFieldType(mapValueType),
                                     versionAnno.family(),
                                     versionAnno.column(),
                                     versionAnno.getter(),
                                     versionAnno.setter(),
                                     versionAnno.mapKeysAsColumns());
        }

    }

    @Override
    public boolean isACurrentValue() {
        return false;
    }

}
