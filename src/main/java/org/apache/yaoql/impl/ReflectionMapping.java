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

package org.apache.yaoql.impl;

import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.schema.Schema;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class ReflectionMapping extends Schema {

    private final static Map<Class<?>, ReflectionMapping> reflectionMappingMap = Maps.newHashMap();

    private ReflectionMapping(final Class clazz) {
        super(clazz.getName(), null);

        for (final Field field : clazz.getDeclaredFields()) {

            if (field.getType().isArray())
                continue;

            if (field.getType().isPrimitive()
                || field.getType().equals(String.class)
                || field.getType().equals(Date.class)) {
                final ReflectionAttrib attrib = new ReflectionAttrib(field);
                try {
                    addAttribToVariableNameMap(attrib, attrib.getVariableName());
                }
                catch (HBqlException e) {
                    // Not going to be hit with ReflectionSchema
                    e.printStackTrace();
                }
            }
        }
    }

    public static ReflectionMapping getReflectionMapping(final Object obj) {
        return getReflectionMapping(obj.getClass());
    }

    public synchronized static ReflectionMapping getReflectionMapping(final Class clazz) {

        ReflectionMapping mapping = getReflectionMappingMap().get(clazz);
        if (mapping != null)
            return mapping;

        mapping = new ReflectionMapping(clazz);
        getReflectionMappingMap().put(clazz, mapping);
        return mapping;
    }

    private static Map<Class<?>, ReflectionMapping> getReflectionMappingMap() {
        return reflectionMappingMap;
    }

    public Collection<String> getSchemaFamilyNames() throws HBqlException {
        return Lists.newArrayList();
    }
}