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

import org.apache.hadoop.hbase.contrib.hbql.client.Column;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class CurrentValueAnnotationAttrib extends FieldAttrib {

    private final Object defaultValue;

    public CurrentValueAnnotationAttrib(final AnnotationSchema parentSchema,
                                        final Field field) throws HBqlException {

        super(field.getAnnotation(Column.class).family(),
              field.getAnnotation(Column.class).column(),
              field,
              FieldType.getFieldType(field),
              field.getAnnotation(Column.class).familyDefault(),
              field.getAnnotation(Column.class).getter(),
              field.getAnnotation(Column.class).setter());

        this.defineAccessors();

        if (isFinal(this.getField()))
            throw new HBqlException(this + "." + this.getField().getName() + " cannot have a @Column "
                                    + "annotation and be marked final");

        defaultValue = getDefaultFieldValue(parentSchema, field);
    }

    private Object getDefaultFieldValue(final AnnotationSchema parentSchema,
                                        final Field field) {
        try {
            return field.get(parentSchema.getSingleInstance());
        }
        catch (IllegalAccessException e) {
            return null;
        }
    }

    private Column getColumnAnno() {
        return this.getField().getAnnotation(Column.class);
    }

    public boolean isAKeyAttrib() {
        return this.getColumnAnno().key();
    }

    private static boolean isFinal(final Field field) {
        return Modifier.isFinal(field.getModifiers());
    }

    public Object getDefaultValue() {
        return this.defaultValue;
    }

    public boolean hasDefaultArg() {
        return this.getDefaultValue() != null;
    }
}
