/*
 * Copyright (c) 2009.  The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.contrib.hbql.schema;

import org.apache.expreval.expr.TypeSupport;
import org.apache.hadoop.hbase.contrib.hbql.client.ColumnVersionMap;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

public class VersionAnnotationAttrib extends FieldAttrib {

    private VersionAnnotationAttrib(final String familyName,
                                    final String columnName,
                                    final Field field,
                                    final FieldType fieldType,
                                    final boolean familyDefault,
                                    final String getter,
                                    final String setter) throws HBqlException {
        super(familyName, columnName, field, fieldType, familyDefault, getter, setter);

        this.defineAccessors();
    }

    public static VersionAnnotationAttrib newVersionAttrib(final HBaseSchema schema, final Field field) throws HBqlException {

        final ColumnVersionMap versionAnno = field.getAnnotation(ColumnVersionMap.class);
        final String instance = versionAnno.instance();

        final String annoname = "@ColumnVersionMap annotation";

        // Check if type is a Map
        if (!TypeSupport.isParentClass(Map.class, field.getType()))
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

            if (versionAnno.familyDefault())
                throw new HBqlException(getObjectQualifiedName(field)
                                        + " cannot have both an instance and familyDefault value in " + annoname);

            // Check if instance variable exists
            if (!schema.containsVariableName(instance))
                throw new HBqlException(annoname + " for " + getObjectQualifiedName(field)
                                        + " refers to invalid instance variable " + instance);

            final ColumnAttrib attrib = schema.getAttribByVariableName(instance);

            if (attrib == null)
                throw new HBqlException("Instance variable " + schema.getSchemaName() + "." + instance + " does not exist");

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
                                               currentAnnoAttrib.isFamilyDefaultAttrib(),
                                               currentAnnoAttrib.getGetter(),
                                               currentAnnoAttrib.getSetter());
        }
        else {

            return new VersionAnnotationAttrib(versionAnno.family(),
                                               versionAnno.column(),
                                               field,
                                               FieldType.getFieldType(mapValueType),
                                               versionAnno.familyDefault(),
                                               versionAnno.getter(),
                                               versionAnno.setter());
        }
    }

    public boolean isACurrentValue() {
        return false;
    }

    public boolean isAVersionValue() {
        return true;
    }
}
