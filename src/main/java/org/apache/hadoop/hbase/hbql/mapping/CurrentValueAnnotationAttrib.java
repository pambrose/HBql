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

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class CurrentValueAnnotationAttrib extends FieldAttrib {

    final ColumnAttrib columnAttrib;

    public CurrentValueAnnotationAttrib(final Field field, final ColumnAttrib columnAttrib) throws HBqlException {

        super(columnAttrib.getFamilyName(),
              columnAttrib.getColumnName(),
              field,
              columnAttrib.getFieldType(),
              columnAttrib.getGetter(),
              columnAttrib.getSetter());

        this.columnAttrib = columnAttrib;

        this.defineAccessors();

        if (isFinal(this.getField()))
            throw new HBqlException(this + "." + this.getField().getName() + " cannot have a @Column "
                                    + "annotation and be marked final");

        // TODO Check for type match

    }

    private ColumnAttrib getColumnAttrib() {
        return columnAttrib;
    }

    public boolean isAKeyAttrib() {
        return this.getColumnAttrib().isAKeyAttrib();
    }

    private static boolean isFinal(final Field field) {
        return Modifier.isFinal(field.getModifiers());
    }

    public Object getDefaultValue() throws HBqlException {
        return this.getColumnAttrib().getDefaultValue();
    }

    public boolean hasDefaultArg() throws HBqlException {
        return this.getDefaultValue() != null;
    }
}
