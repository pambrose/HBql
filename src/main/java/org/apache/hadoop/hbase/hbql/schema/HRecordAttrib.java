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

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.RecordImpl;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class HRecordAttrib extends ColumnAttrib {

    public HRecordAttrib(final ColumnDescription columnDescription) throws HBqlException {

        super(columnDescription.getFamilyName(),
              columnDescription.getColumnName(),
              columnDescription.getAliasName(),
              columnDescription.isFamilyDefault(),
              columnDescription.getFieldType(),
              columnDescription.isArray(),
              null,
              null,
              columnDescription.getDefaultValue());

        if (this.isAKeyAttrib() && this.getFamilyName().length() > 0)
            throw new HBqlException("Key value " + this.getNameToUseInExceptions() + " cannot have a family name");
    }

    public String asString() throws HBqlException {

        final StringBuilder sbuf = new StringBuilder();

        sbuf.append(this.getFamilyQualifiedName());

        sbuf.append(" " + this.getFieldType().getFirstSynonym());

        if (this.isAnArray())
            sbuf.append("[]");

        if (this.hasAlias())
            sbuf.append(" ALIAS " + this.getAliasName());

        if (this.hasDefaultArg())
            sbuf.append(" DEFAULT " + this.getDefaultArg().asString());

        return sbuf.toString();
    }

    public String toString() {
        return this.getAliasName() + " - " + this.getFamilyQualifiedName();
    }

    public boolean isAKeyAttrib() {
        return this.getFieldType() == FieldType.KeyType;
    }

    protected void defineAccessors() {
        // TODO This needs to be implemented
    }

    public Object getCurrentValue(final Object record) throws HBqlException {
        return ((RecordImpl)record).getCurrentValue(this.getAliasName());
    }

    public void setCurrentValue(final Object record, final long timestamp, final Object val) throws HBqlException {
        ((RecordImpl)record).setCurrentValue(this.getAliasName(), timestamp, val, true);
    }

    public Map<Long, Object> getVersionMap(final Object record) throws HBqlException {
        return ((RecordImpl)record).getColumnValue(this.getAliasName(), true).getVersionMap(true);
    }

    public void setFamilyDefaultCurrentValue(final Object record,
                                             final String name,
                                             final byte[] value) throws HBqlException {
        ((RecordImpl)record).setFamilyDefaultCurrentValue(this.getFamilyName(), name, 0, value);
    }

    public void setFamilyDefaultVersionMap(final Object record,
                                           final String name,
                                           final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException {
        ((RecordImpl)record).setFamilyDefaultVersionMap(this.getFamilyName(), name, timeStampMap);
    }

    protected Method getMethod(final String methodName,
                               final Class<?>... params) throws NoSuchMethodException, HBqlException {
        throw new InternalErrorException();
    }

    protected Class getComponentType() {
        return this.getFieldType().getComponentType();
    }

    public String getNameToUseInExceptions() {
        return this.getFamilyQualifiedName();
    }

    public String getEnclosingClassName() {
        // TODO This will get resolved when getter/setter is added to HBaseSchema
        return "";
    }

    public boolean isAVersionValue() {
        return true;
    }

    public String[] getNamesForColumn() {
        final List<String> nameList = Lists.newArrayList();
        nameList.add(this.getFamilyQualifiedName());
        if (!this.getAliasName().equals(this.getFamilyQualifiedName()))
            nameList.add(this.getAliasName());
        return nameList.toArray(new String[nameList.size()]);
    }
}
