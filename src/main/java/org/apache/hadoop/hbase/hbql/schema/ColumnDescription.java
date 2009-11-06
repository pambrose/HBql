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

import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.io.Serializable;

public final class ColumnDescription implements Serializable {

    private final String aliasName;
    private final String familyName, columnName;
    private final boolean familyDefault;
    private final boolean isArray;
    private final FieldType fieldType;
    private final GenericValue defaultValue;

    private ColumnDescription(final String familyQualifiedName,
                              final String aliasName,
                              final boolean familyDefault,
                              final String typeName,
                              final boolean isArray,
                              final GenericValue defaultValue) {

        if (familyQualifiedName.indexOf(":") != -1) {
            final String[] names = familyQualifiedName.split(":");
            familyName = names[0];
            columnName = names[1];
        }
        else {
            familyName = "";
            columnName = familyQualifiedName;
        }

        this.aliasName = aliasName;
        this.familyDefault = familyDefault;
        this.fieldType = getFieldType(typeName);
        this.isArray = isArray;
        this.defaultValue = defaultValue;
    }

    public static ColumnDescription newColumn(final String familyQualifiedName,
                                              final String aliasName,
                                              final boolean familyDefault,
                                              final String typeName,
                                              final boolean isArray,
                                              final GenericValue defaultValue) {
        return new ColumnDescription(familyQualifiedName,
                                     aliasName,
                                     familyDefault,
                                     typeName,
                                     isArray,
                                     defaultValue);
    }

    public static ColumnDescription newFamilyDefault(final String familyQualifiedName, final String aliasName) {
        return new ColumnDescription(familyQualifiedName, aliasName, true, null, false, null);
    }

    private static FieldType getFieldType(final String typeName) {
        try {
            return FieldType.getFieldType(typeName);
        }
        catch (HBqlException e) {
            return null;
        }
    }

    public String getFamilyName() {
        return this.familyName;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public String getAliasName() {
        return this.aliasName;
    }

    public boolean isFamilyDefault() {
        return this.familyDefault;
    }

    public FieldType getFieldType() {
        return this.fieldType;
    }

    public boolean isArray() {
        return this.isArray;
    }

    public GenericValue getDefaultValue() {
        return this.defaultValue;
    }
}


