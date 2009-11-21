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


public final class ColumnDefinition implements Serializable {

    private final String columnName;
    private final String aliasName;
    private final boolean isArray;
    private final FieldType fieldType;
    private final GenericValue defaultValue;

    private FamilyMapping familyMapping = null;

    public ColumnDefinition(final String columnName,
                            final String typeName,
                            final boolean isArray,
                            final String aliasName,
                            final GenericValue defaultValue) {

        this.columnName = columnName;
        this.aliasName = aliasName;
        this.fieldType = getFieldType(typeName);
        this.isArray = isArray;
        this.defaultValue = defaultValue;
    }

    // For KEY Attribs
    public ColumnDefinition(final String keyName) {
        this.columnName = keyName;
        this.aliasName = keyName;
        this.fieldType = FieldType.KeyType;
        this.isArray = false;
        this.defaultValue = null;
        this.familyMapping = new FamilyMapping("", null, false);
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