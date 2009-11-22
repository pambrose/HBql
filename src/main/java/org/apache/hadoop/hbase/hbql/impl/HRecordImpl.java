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

package org.apache.hadoop.hbase.hbql.impl;

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.client.FamilyDefaultValueMap;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.HBaseMapping;
import org.apache.hadoop.hbase.hbql.mapping.ResultMapping;
import org.apache.hadoop.hbase.hbql.statement.MappingContext;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public class HRecordImpl implements Serializable, HRecord {

    private MappingContext mappingContext;
    private long timestamp = System.currentTimeMillis();

    private List<String> namePositionList = Lists.newArrayList();

    private volatile ElementMap<ColumnValue> columnValuesMap = null;
    private volatile ElementMap<FamilyDefaultValueMap> familyDefaultElementsMap = null;

    public HRecordImpl(final MappingContext mappingContext) {
        this.setMappingContext(mappingContext);
    }

    public MappingContext getMappingContext() {
        return mappingContext;
    }

    public void setMappingContext(final MappingContext mappingContext) {
        this.mappingContext = mappingContext;
    }

    public String getAttribName(final int i) throws HBqlException {

        try {
            return this.getNamePositionList().get(i - 1);
        }
        catch (ArrayIndexOutOfBoundsException e) {
            throw new HBqlException("Invalid column number " + i);
        }
    }

    private List<String> getNamePositionList() {
        return this.namePositionList;
    }

    public void addNameToPositionList(final String name) {
        this.getNamePositionList().add(name);
    }

    public HBaseMapping getHBaseMapping() throws HBqlException {
        return this.getMappingContext().getHBaseMapping();
    }

    public ResultMapping getResultMapping() throws HBqlException {
        return this.getMappingContext().getResultMapping();
    }

    protected ElementMap<ColumnValue> getColumnValuesMap() {
        if (this.columnValuesMap == null)
            synchronized (this) {
                if (this.columnValuesMap == null)
                    this.columnValuesMap = new ElementMap<ColumnValue>(this);
            }
        return this.columnValuesMap;
    }

    private ElementMap<FamilyDefaultValueMap> getFamilyDefaultElementsMap() {
        if (this.familyDefaultElementsMap == null)
            synchronized (this) {
                if (this.familyDefaultElementsMap == null)
                    this.familyDefaultElementsMap = new ElementMap<FamilyDefaultValueMap>(this);
            }
        return this.familyDefaultElementsMap;
    }

    public void addElement(final Value value) throws HBqlException {

        if (value instanceof ColumnValue)
            this.getColumnValuesMap().addElement((ColumnValue)value);
        else if (value instanceof FamilyDefaultValueMap)
            this.getFamilyDefaultElementsMap().addElement((FamilyDefaultValueMap)value);
        else
            throw new InternalErrorException(value.getClass().getName());
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void clearValues() {
        this.getColumnValuesMap().clear();
        this.getFamilyDefaultElementsMap().clear();
    }

    // Simple get routines
    public ColumnValue getColumnValue(final String name, final boolean inMapping) throws HBqlException {
        final ColumnValue value = this.getColumnValuesMap().findElement(name);
        if (value != null) {
            return value;
        }
        else {
            if (inMapping && !this.getHBaseMapping().containsVariableName(name))
                throw new HBqlException("Invalid variable name " + this.getHBaseMapping()
                        .getMappingName() + "." + name);
            final ColumnValue columnValue = new ColumnValue(name);
            this.addElement(columnValue);
            return columnValue;
        }
    }

    private FamilyDefaultValueMap getFamilyDefaultValueMap(final String name,
                                                           final boolean createNewIfMissing) throws HBqlException {
        final FamilyDefaultValueMap value = this.getFamilyDefaultElementsMap().findElement(name);
        if (value != null) {
            return value;
        }
        else {
            if (createNewIfMissing) {
                final FamilyDefaultValueMap val = new FamilyDefaultValueMap(name);
                this.addElement(val);
                return val;
            }
            else {
                return null;
            }
        }
    }

    // Current Object values
    public void setCurrentValue(final String family,
                                final String column,
                                final long timestamp,
                                final Object val) throws HBqlException {
        final ColumnAttrib attrib = this.getHBaseMapping().getAttribFromFamilyQualifiedName(family, column);
        if (attrib == null)
            throw new HBqlException("Invalid column name " + family + ":" + column);
        this.setCurrentValue(attrib.getAliasName(), timestamp, val, true);
    }

    public boolean isCurrentValueSet(final ColumnAttrib attrib) throws HBqlException {
        final ColumnValue columnValue = this.getColumnValuesMap().findElement(attrib.getAliasName());
        return columnValue != null && columnValue.isValueSet();
    }

    public void setCurrentValue(final String name,
                                final long timestamp,
                                final Object val,
                                final boolean inMapping) throws HBqlException {
        this.getColumnValue(name, inMapping).setCurrentValue(timestamp, val);
    }

    public void setVersionValue(final String familyName,
                                final String columnName,
                                final long timestamp,
                                final Object val,
                                final boolean inMapping) throws HBqlException {
        final ColumnAttrib attrib = this.getHBaseMapping().getAttribFromFamilyQualifiedName(familyName, columnName);
        if (attrib == null)
            throw new HBqlException("Invalid column name " + familyName + ":" + columnName);

        this.getColumnValue(attrib.getColumnName(), inMapping).getVersionMap(true).put(timestamp, val);
    }

    public void setFamilyDefaultCurrentValue(final String familyName,
                                             final String name,
                                             final long timestamp,
                                             final byte[] val) throws HBqlException {
        this.getFamilyDefaultValueMap(familyName + ":*", true).setCurrentValueMap(timestamp, name, val);
    }

    public void setFamilyDefaultVersionMap(final String familyName,
                                           final String name,
                                           final NavigableMap<Long, byte[]> val) throws HBqlException {
        this.getFamilyDefaultValueMap(familyName + ":*", true).setVersionMap(name, val);
    }

    public void reset() {
        this.columnValuesMap = null;
        this.familyDefaultElementsMap = null;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public void setCurrentValue(final String name, final Object val) throws HBqlException {
        this.setCurrentValue(name, this.getTimestamp(), val, true);
    }

    public Object getCurrentValue(final String name) throws HBqlException {
        final ColumnValue columnValue = this.getColumnValuesMap().findElement(name);
        if (columnValue != null) {
            final Object retval = columnValue.getCurrentValue();
            if (retval != null)
                return retval;
        }

        // Return default value if it exists
        final ColumnAttrib attrib = this.getMappingContext().getResultMapping().getAttribByVariableName(name);
        return (attrib != null) ? attrib.getDefaultValue() : null;
    }

    public Set<String> getColumnNameList() throws HBqlException {
        return this.getColumnValuesMap().keySet();
    }

    public Map<Long, Object> getVersionMap(final String name) throws HBqlException {
        final ColumnValue value = this.getColumnValuesMap().findElement(name);
        return (value != null) ? value.getVersionMap(true) : null;
    }

    public Map<String, byte[]> getFamilyDefaultValueMap(final String name) throws HBqlException {

        final FamilyDefaultValueMap value = this.getFamilyDefaultValueMap(name, false);
        if (value == null)
            return null;

        final Map<String, byte[]> retval = Maps.newHashMap();
        for (final String key : value.getCurrentAndVersionMap().keySet())
            retval.put(key, value.getCurrentAndVersionMap().get(key).getCurrentValue());
        return retval;
    }

    public Map<String, NavigableMap<Long, byte[]>> getFamilyDefaultVersionMap(final String name) throws HBqlException {

        final FamilyDefaultValueMap value = this.getFamilyDefaultValueMap(name, false);
        if (value == null)
            return null;

        final Map<String, NavigableMap<Long, byte[]>> retval = Maps.newHashMap();
        for (final String key : value.getCurrentAndVersionMap().keySet())
            retval.put(key, value.getCurrentAndVersionMap().get(key).getVersionMap(true));
        return retval;
    }
}
