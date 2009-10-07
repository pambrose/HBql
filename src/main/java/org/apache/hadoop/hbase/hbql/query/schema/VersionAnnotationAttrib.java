package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HColumnVersionMap;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

public class VersionAnnotationAttrib extends FieldAttrib {

    private VersionAnnotationAttrib(final String familyName,
                                    final String columnName,
                                    final Field field,
                                    final FieldType fieldType,
                                    final boolean mapKeysAsColumns,
                                    final String getter,
                                    final String setter) throws HBqlException {
        super(familyName, columnName, field, fieldType, mapKeysAsColumns, getter, setter);

        this.defineAccessors();
    }

    public static VersionAnnotationAttrib newVersionAttrib(final HBaseSchema schema, final Field field) throws HBqlException {

        final HColumnVersionMap versionAnno = field.getAnnotation(HColumnVersionMap.class);
        final String instance = versionAnno.instance();

        final String annoname = "@HColumnVersionMap annotation";

        // Check if type is a Map
        if (!HUtil.isParentClass(Map.class, field.getType()))
            throw new HBqlException(getObjectQualifiedName(field) + "has a " + annoname + " so it "
                                    + "must implement the Map interface");

        // Look up type of map value
        final ParameterizedType ptype = (ParameterizedType)field.getGenericType();
        final Type[] typeargs = ptype.getActualTypeArguments();
        final Type mapValueType = typeargs[1];

        if (instance.length() > 0) {
            if (versionAnno.family().length() > 0)
                throw new HBqlException(getObjectQualifiedName(field)
                                        + " cannot have both an instance and family value in " + annoname);
            if (versionAnno.column().length() > 0)
                throw new HBqlException(getObjectQualifiedName(field)
                                        + " cannot have both an instance and column value in " + annoname);
            if (versionAnno.getter().length() > 0)
                throw new HBqlException(getObjectQualifiedName(field)
                                        + " cannot have both an instance and getter value in " + annoname);
            if (versionAnno.setter().length() > 0)
                throw new HBqlException(getObjectQualifiedName(field)
                                        + " cannot have both an instance and setter value in " + annoname);

            // This doesn't test false values -- they wil be ignored
            if (versionAnno.mapKeysAsColumns())
                throw new HBqlException(getObjectQualifiedName(field)
                                        + " cannot have both an instance and mapKeysAsColumns value in " + annoname);

            // Check if instance variable exists
            if (!schema.constainsVariableName(instance))
                throw new HBqlException(annoname + " for " + getObjectQualifiedName(field)
                                        + " refers to invalid instance variable " + instance);

            final ColumnAttrib attrib = schema.getAttribByVariableName(instance);

            if (attrib == null)
                throw new HBqlException("Instance variable " + instance
                                        + " does not exist in " + schema.getTableName());

            if (!attrib.isACurrentValue())
                throw new HBqlException(getObjectQualifiedName(field)
                                        + "instance variable must have HColumn annotation");

            final CurrentValueAnnotationAttrib currentAnnoAttrib = (CurrentValueAnnotationAttrib)attrib;

            // Make sure type of Value in map matches type of instance var
            if (!mapValueType.equals(currentAnnoAttrib.getField().getType()))
                throw new HBqlException("Type of " + getObjectQualifiedName(field) + " map value type does not " +
                                        "match type of " + currentAnnoAttrib.getObjectQualifiedName());

            return new VersionAnnotationAttrib(currentAnnoAttrib.getFamilyName(),
                                               currentAnnoAttrib.getColumnName(),
                                               field,
                                               currentAnnoAttrib.getFieldType(),
                                               currentAnnoAttrib.isMapKeysAsColumns(),
                                               currentAnnoAttrib.getGetter(),
                                               currentAnnoAttrib.getSetter());
        }
        else {
            return new VersionAnnotationAttrib(versionAnno.family(),
                                               versionAnno.column(),
                                               field,
                                               FieldType.getFieldType(mapValueType),
                                               versionAnno.mapKeysAsColumns(),
                                               versionAnno.getter(),
                                               versionAnno.setter());
        }
    }

    public boolean isACurrentValue() {
        return false;
    }
}
