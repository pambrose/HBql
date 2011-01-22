/*
 * Copyright (c) 2011.  The Apache Software Foundation
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

package org.apache.hadoop.hbase.hbql.mapping;

import org.apache.expreval.expr.TypeSupport;
import org.apache.hadoop.hbase.hbql.client.ColumnVersionMap;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

public class VersionAnnotationAttrib extends FieldAttrib {

    public VersionAnnotationAttrib(final String familyName,
                                   final String columnName,
                                   final Field field,
                                   final FieldType fieldType,
                                   final String getter,
                                   final String setter) throws HBqlException {
        super(familyName, columnName, field, fieldType, getter, setter);

        final ColumnVersionMap versionAnno = field.getAnnotation(ColumnVersionMap.class);

        final String annoname = "@ColumnVersionMap annotation";

        // Check if type is a Map
        if (!TypeSupport.isParentClass(Map.class, field.getType()))
            throw new HBqlException(getObjectQualifiedName(field) + "has a " + annoname + " so it "
                                    + "must implement the Map interface");

        // Look up type of map value
        final ParameterizedType ptype = (ParameterizedType)field.getGenericType();
        final Type[] typeargs = ptype.getActualTypeArguments();
        final Type mapValueType = typeargs[1];

        // TODO Deal with type check on mapValueType

        this.defineAccessors();
    }

    public boolean isACurrentValue() {
        return false;
    }

    public boolean isAVersionValue() {
        return true;
    }
}
