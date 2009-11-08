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

package org.apache.hadoop.hbase.hbql.statement.select;

import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.Connection;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.AggregateValue;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.schema.SelectFamilyAttrib;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;

import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

public class FamilySelectElement implements SelectElement {

    private final boolean useAllFamilies;
    private final List<String> familyNameList = Lists.newArrayList();
    private final List<byte[]> familyNameBytesList = Lists.newArrayList();
    private final List<ColumnAttrib> attribsUsedInExpr = Lists.newArrayList();
    private final String familyName;

    private HBaseSchema schema;

    public FamilySelectElement(final String familyName) {

        this.familyName = familyName;

        if (familyName.equals("*")) {
            this.useAllFamilies = true;
        }
        else {
            this.useAllFamilies = false;
            this.addAFamily(familyName.replaceAll(" ", "").replace(":*", ""));
        }
    }

    public void addAFamily(final String familyName) {
        this.familyNameList.add(familyName);
    }

    public static List<SelectElement> newAllFamilies() {
        final List<SelectElement> retval = Lists.newArrayList();
        retval.add(new FamilySelectElement("*"));
        return retval;
    }

    public static FamilySelectElement newFamilyElement(final String family) {
        return new FamilySelectElement(family);
    }

    public List<String> getFamilyNameList() {
        return this.familyNameList;
    }

    public List<byte[]> getFamilyNameBytesList() {
        return this.familyNameBytesList;
    }

    protected HBaseSchema getSchema() {
        return this.schema;
    }

    public String getAsName() {
        return null;
    }

    public String getElementName() {
        return null;
    }

    public boolean hasAsName() {
        return false;
    }

    public boolean isAFamilySelect() {
        return true;
    }

    public String asString() {
        return this.familyName;
    }

    public boolean isAnAggregateElement() {
        return false;
    }

    public int setParameter(final String name, final Object val) {
        // Do nothing
        return 0;
    }

    public void validate(final HBaseSchema schema, final Connection connection) throws HBqlException {

        this.schema = schema;
        this.getAttribsUsedInExpr().clear();
        final Collection<String> familyList = this.getSchema().getSchemaFamilyNames(connection);

        if (this.useAllFamilies) {
            // connction will be null from tests
            for (final String familyName : familyList) {
                this.addAFamily(familyName);
                this.attribsUsedInExpr.add(new SelectFamilyAttrib(familyName));
            }
        }
        else {
            // Only has one family
            final String familyName = this.getFamilyNameList().get(0);
            if (!familyList.contains(familyName))
                throw new HBqlException("Invalid family name: " + familyName);

            this.getAttribsUsedInExpr().add(new SelectFamilyAttrib(familyName));
        }

        for (final String familyName : this.getFamilyNameList())
            this.getFamilyNameBytesList().add(IO.getSerialization().getStringAsBytes(familyName));
    }


    public List<ColumnAttrib> getAttribsUsedInExpr() {
        return this.attribsUsedInExpr;
    }

    public void assignAsNamesForExpressions(final SelectStatement selectStatement) {
        // No op
    }

    public AggregateValue newAggregateValue() throws HBqlException {
        // No op
        return null;
    }

    public void assignSelectValue(final Object obj,
                                  final int maxVersions,
                                  final Result result) throws HBqlException {

        final HBaseSchema schema = this.getSchema();

        // Evaluate each of the families (select * will yield all families)
        for (int i = 0; i < this.getFamilyNameBytesList().size(); i++) {

            final String familyName = this.getFamilyNameList().get(i);
            final byte[] familyNameBytes = this.getFamilyNameBytesList().get(i);

            final NavigableMap<byte[], byte[]> columnMap = result.getFamilyMap(familyNameBytes);

            for (final byte[] columnBytes : columnMap.keySet()) {

                final byte[] valueBytes = columnMap.get(columnBytes);
                final String columnName = IO.getSerialization().getStringFromBytes(columnBytes);

                final ColumnAttrib attrib = schema.getAttribFromFamilyQualifiedName(familyName, columnName);
                if (attrib == null) {
                    final ColumnAttrib familyDefaultAttrib = schema.getFamilyDefault(familyName);
                    if (familyDefaultAttrib != null)
                        familyDefaultAttrib.setFamilyDefaultCurrentValue(obj, columnName, valueBytes);
                }
                else {
                    attrib.setCurrentValue(obj, 0, valueBytes);
                }
            }

            // Bail if no versions were requested
            if (maxVersions <= 1)
                continue;

            final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();
            final NavigableMap<byte[], NavigableMap<Long, byte[]>> versionColumnMap = familyMap.get(familyNameBytes);

            if (versionColumnMap == null)
                continue;

            for (final byte[] columnBytes : versionColumnMap.keySet()) {

                final NavigableMap<Long, byte[]> timeStampMap = versionColumnMap.get(columnBytes);
                final String columnName = IO.getSerialization().getStringFromBytes(columnBytes);

                final ColumnAttrib attrib = schema.getVersionAttribMap(familyName, columnName);

                if (attrib == null) {
                    final ColumnAttrib familyDefaultAttrib = schema.getFamilyDefault(familyName);
                    if (familyDefaultAttrib != null)
                        familyDefaultAttrib.setFamilyDefaultVersionMap(obj, columnName, timeStampMap);
                }
                else {
                    attrib.setVersionMap(obj, timeStampMap);
                }
            }
        }
    }
}
