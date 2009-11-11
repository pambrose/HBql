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

package org.apache.hadoop.hbase.hbql.schema;

import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class ReflectionSchema extends Schema {

    private final static Map<Class<?>, ReflectionSchema> reflectionSchemaMap = Maps.newHashMap();

    private ReflectionSchema(final Class clazz) throws HBqlException {
        super(clazz.getName());

        for (final Field field : clazz.getDeclaredFields()) {

            if (field.getType().isArray())
                continue;

            if (field.getType().isPrimitive()
                || field.getType().equals(String.class)
                || field.getType().equals(Date.class)) {
                final ReflectionAttrib attrib = new ReflectionAttrib(field);
                addAttribToVariableNameMap(attrib, attrib.getVariableName());
            }
        }
    }

    public static ReflectionSchema getReflectionSchema(final Object obj) throws HBqlException {
        return getReflectionSchema(obj.getClass());
    }

    public synchronized static ReflectionSchema getReflectionSchema(final Class clazz) throws HBqlException {

        ReflectionSchema schema = getReflectionSchemaMap().get(clazz);
        if (schema != null)
            return schema;

        schema = new ReflectionSchema(clazz);
        getReflectionSchemaMap().put(clazz, schema);
        return schema;
    }

    private static Map<Class<?>, ReflectionSchema> getReflectionSchemaMap() {
        return reflectionSchemaMap;
    }

    public Collection<String> getSchemaFamilyNames(final HConnection connection) throws HBqlException {
        return Lists.newArrayList();
    }
}