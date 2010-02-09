/*
 * Copyright (c) 2010.  The Apache Software Foundation
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

import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.ByteValue;
import org.apache.expreval.expr.node.CharValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.DoubleValue;
import org.apache.expreval.expr.node.FloatValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.IntegerValue;
import org.apache.expreval.expr.node.LongValue;
import org.apache.expreval.expr.node.ObjectValue;
import org.apache.expreval.expr.node.ShortValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.client.idx.IdxQualifierType;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.util.Lists;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public enum FieldType implements Serializable {

    KeyType(String.class, StringValue.class, -1, -1, null),  // No type synonyms for KEY

    BooleanType(Boolean.TYPE, BooleanValue.class, 0, Bytes.SIZEOF_BOOLEAN, null, "BOOLEAN", "BOOL"),
    ByteType(Byte.TYPE, ByteValue.class, 1, Bytes.SIZEOF_BYTE, IdxQualifierType.BYTE, "BYTE"),
    CharType(Character.TYPE, CharValue.class, 1, Bytes.SIZEOF_CHAR, IdxQualifierType.CHAR, "CHAR"),

    ShortType(Short.TYPE, ShortValue.class, 2, Bytes.SIZEOF_SHORT, IdxQualifierType.SHORT, "SHORT"),
    IntegerType(Integer.TYPE, IntegerValue.class, 3, Bytes.SIZEOF_INT, IdxQualifierType.INT, "INTEGER", "INT"),
    LongType(Long.TYPE, LongValue.class, 4, Bytes.SIZEOF_LONG, IdxQualifierType.LONG, "LONG"),
    FloatType(Float.TYPE, FloatValue.class, 5, Bytes.SIZEOF_FLOAT, IdxQualifierType.FLOAT, "FLOAT"),
    DoubleType(Double.TYPE, DoubleValue.class, 6, Bytes.SIZEOF_DOUBLE, IdxQualifierType.DOUBLE, "DOUBLE"),

    StringType(String.class, StringValue.class, -1, -1, IdxQualifierType.BYTE_ARRAY, "STRING", "VARCHAR"),
    DateType(Date.class, DateValue.class, -1, -1, null, "DATE", "DATETIME"),
    ObjectType(Object.class, ObjectValue.class, -1, -1, null, "OBJECT", "OBJ");

    private Class componentType;
    private Class<? extends GenericValue> exprType;
    private int typeRanking;
    private int size;
    private IdxQualifierType indexType;
    private List<String> synonymList;

    FieldType() {
    }

    FieldType(final Class componentType,
              final Class<? extends GenericValue> exprType,
              final int typeRanking,
              final int size,
              final IdxQualifierType indexType,
              final String... synonyms) {
        this.componentType = componentType;
        this.exprType = exprType;
        this.typeRanking = typeRanking;
        this.size = size;
        this.indexType = indexType;
        this.synonymList = Lists.newArrayList();
        this.synonymList.addAll(Arrays.asList(synonyms));
    }

    public Class getComponentType() {
        return this.componentType;
    }

    public int getTypeRanking() {
        return this.typeRanking;
    }

    public int getSize() {
        return this.size;
    }

    public IdxQualifierType getIndexType() {
        return this.indexType;
    }

    public Class<? extends GenericValue> getExprType() {
        return this.exprType;
    }

    public static FieldType getFieldType(final Object obj) {
        final Class fieldClass = obj.getClass();
        return getFieldType(fieldClass);
    }

    public static FieldType getFieldType(final Field field) {
        final Class fieldClass = field.getType();
        return getFieldType(fieldClass);
    }

    public String getFirstSynonym() {
        return this.getSynonymList().get(0);
    }

    private List<String> getSynonymList() {
        return this.synonymList;
    }

    public static FieldType getFieldType(final Class fieldClass) {

        final Class<?> clazz = fieldClass.isArray() ? fieldClass.getComponentType() : fieldClass;

        if (!clazz.isPrimitive()) {
            if (clazz.equals(String.class))
                return StringType;
            else if (clazz.equals(Date.class))
                return DateType;
            else
                return ObjectType;
        }
        else {
            for (final FieldType type : values()) {
                final Class compType = type.getComponentType();
                if (clazz.equals(compType))
                    return type;
            }
        }

        throw new RuntimeException("Unknown type: " + clazz + " in FieldType.getFieldType()");
    }

    public static FieldType getFieldType(final String desc) throws HBqlException {

        if (desc == null)
            return null;

        for (final FieldType type : values()) {
            if (type.matchesSynonym(desc))
                return type;
        }

        throw new HBqlException("Unknown type description: " + desc);
    }

    private boolean matchesSynonym(final String str) {
        for (final String syn : this.getSynonymList())
            if (str.equalsIgnoreCase(syn))
                return true;
        return false;
    }
}
