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

package org.apache.hadoop.hbase.hbql.mapping;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.io.Serializable;


public final class ColumnDefinition implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String columnName;
    private final String aliasName;
    private final boolean isArray;
    private final FieldType fieldType;
    private final GenericValue defaultValue;

    private FamilyMapping familyMapping = null;

    private ColumnDefinition(final String columnName,
                             final FieldType fieldType,
                             final boolean isArray,
                             final String aliasName,
                             final GenericValue defaultValue) {

        this.columnName = columnName;
        this.fieldType = fieldType;
        this.isArray = isArray;
        this.aliasName = aliasName;
        this.defaultValue = defaultValue;
    }

    // For KEY attribs
    public static ColumnDefinition newKeyColumn(final String keyName) {
        final ColumnDefinition column = new ColumnDefinition(keyName, FieldType.KeyType, false, keyName, null);
        column.setFamilyMapping(new FamilyMapping("", false, null));
        return column;
    }

    // For Family Default attribs
    public static ColumnDefinition newUnMappedColumn(final String familyName) {
        final ColumnDefinition column = new ColumnDefinition("", null, false, familyName, null);
        column.setFamilyMapping(new FamilyMapping(familyName, false, null));
        return column;
    }

    // For regular attribs
    public static ColumnDefinition newMappedColumn(final String columnName,
                                                   final String typeName,
                                                   final boolean isArray,
                                                   final String aliasName,
                                                   final GenericValue defaultValue) {
        return new ColumnDefinition(columnName, getFieldType(typeName), isArray, aliasName, defaultValue);
    }

    private FamilyMapping getFamilyMapping() {
        return this.familyMapping;
    }

    void setFamilyMapping(final FamilyMapping familyMapping) {
        this.familyMapping = familyMapping;
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
        return this.getFamilyMapping().getFamilyName();
    }

    public String getColumnName() {
        return this.columnName;
    }

    public String getAliasName() {
        return this.aliasName;
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