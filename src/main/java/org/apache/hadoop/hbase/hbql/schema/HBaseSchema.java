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

import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.impl.RecordImpl;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.parser.HBqlShell;
import org.apache.hadoop.hbase.hbql.statement.select.SelectElement;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class HBaseSchema extends Schema {

    private ColumnAttrib keyAttrib = null;
    private final String tableName;

    private Set<String> familyNameSet = null;

    private final Map<String, ColumnAttrib> columnAttribByFamilyQualifiedNameMap = Maps.newHashMap();
    private final Map<String, ColumnAttrib> versionAttribMap = Maps.newHashMap();
    private final Map<String, ColumnAttrib> familyDefaultMap = Maps.newHashMap();
    private final Map<String, List<ColumnAttrib>> columnAttribListByFamilyNameMap = Maps.newHashMap();

    public HBaseSchema(final List<ColumnDescription> columnDescriptionList) throws HBqlException {
        super("embedded");
        this.tableName = "embedded";
        for (final ColumnDescription columnDescription : columnDescriptionList)
            this.processColumn(columnDescription, false);
    }

    public HBaseSchema(final String schemaName,
                       final String tableName,
                       final List<ColumnDescription> columnDescriptionList) throws HBqlException {
        super(schemaName);
        this.tableName = tableName;
        for (final ColumnDescription columnDescription : columnDescriptionList)
            processColumn(columnDescription, true);
    }

    public HBaseSchema(final String schemaName,
                       final String tableName) throws HBqlException {
        super(schemaName);
        this.tableName = tableName;
    }

    public Object newInstance() throws IllegalAccessException, InstantiationException {
        return null;
    }

    public ColumnAttrib getKeyAttrib() {
        return this.keyAttrib;
    }

    protected void setKeyAttrib(final ColumnAttrib keyAttrib) {
        this.keyAttrib = keyAttrib;
    }

    public String getTableName() {
        return this.tableName;
    }

    public List<HColumnDescriptor> getColumnDescriptors() {
        final List<HColumnDescriptor> descList = Lists.newArrayList();
        for (final String familyName : this.getFamilySet())
            descList.add(new HColumnDescriptor(familyName));
        return descList;
    }


    public byte[] getTableNameAsBytes() throws HBqlException {
        return IO.getSerialization().getStringAsBytes(this.getTableName());
    }

    public Object newObject(final List<SelectElement> selectElementList,
                            final int maxVersions,
                            final Result result) throws HBqlException {

        // Create object and assign values
        final RecordImpl newrec = new RecordImpl(this);
        this.assignSelectValues(newrec, selectElementList, maxVersions, result);
        return newrec;
    }

    private void assignSelectValues(final Object newobj,
                                    final List<SelectElement> selectElementList,
                                    final int maxVersions,
                                    final Result result) throws HBqlException {

        // Set key value
        this.getKeyAttrib().setCurrentValue(newobj, 0, result.getRow());

        // Set the non-key values
        for (final SelectElement selectElement : selectElementList)
            selectElement.assignSelectValue(newobj, maxVersions, result);
    }

    // *** columnAttribByFamilyQualifiedNameMap calls
    protected Map<String, ColumnAttrib> getAttribByFamilyQualifiedNameMap() {
        return this.columnAttribByFamilyQualifiedNameMap;
    }

    public ColumnAttrib getAttribFromFamilyQualifiedName(final String familyName, final String columnName) {
        return this.getAttribFromFamilyQualifiedName(familyName + ":" + columnName);
    }

    public ColumnAttrib getAttribFromFamilyQualifiedName(final String familyQualifiedName) {
        return this.getAttribByFamilyQualifiedNameMap().get(familyQualifiedName);
    }

    protected void addAttribToFamilyQualifiedNameMap(final ColumnAttrib attrib) throws HBqlException {

        if (attrib.isFamilyDefaultAttrib())
            return;

        final String name = attrib.getFamilyQualifiedName();
        if (this.getAttribByFamilyQualifiedNameMap().containsKey(name))
            throw new HBqlException(name + " already declared");
        this.getAttribByFamilyQualifiedNameMap().put(name, attrib);
    }

    // *** familyDefaultMap calls
    private Map<String, ColumnAttrib> getFamilyDefaultMap() {
        return this.familyDefaultMap;
    }

    public ColumnAttrib getFamilyDefault(final String name) {
        return this.getFamilyDefaultMap().get(name);
    }

    protected void addFamilyDefaultAttrib(final ColumnAttrib attrib) throws HBqlException {

        if (!attrib.isFamilyDefaultAttrib())
            return;

        final String familyName = attrib.getFamilyName();
        if (this.getFamilyDefaultMap().containsKey(familyName))
            throw new HBqlException(familyName + " already declared");

        this.getFamilyDefaultMap().put(familyName, attrib);

        final String aliasName = attrib.getAliasName();
        if (aliasName == null || aliasName.length() == 0 || aliasName.equals(familyName))
            return;

        if (this.getFamilyDefaultMap().containsKey(aliasName))
            throw new HBqlException(aliasName + " already declared");

        this.getFamilyDefaultMap().put(aliasName, attrib);
    }

    // *** versionAttribByFamilyQualifiedNameMap calls
    private Map<String, ColumnAttrib> getVersionAttribMap() {
        return this.versionAttribMap;
    }

    public ColumnAttrib getVersionAttrib(final String name) {
        return this.getVersionAttribMap().get(name);
    }

    public ColumnAttrib getVersionAttribMap(final String familyName, final String columnName) {
        return this.getVersionAttrib(familyName + ":" + columnName);
    }

    protected void addVersionAttrib(final ColumnAttrib attrib) throws HBqlException {

        if (!attrib.isAVersionValue())
            return;

        final String familyQualifiedName = attrib.getFamilyQualifiedName();
        if (this.getVersionAttribMap().containsKey(familyQualifiedName))
            throw new HBqlException(familyQualifiedName + " already declared");

        this.getVersionAttribMap().put(familyQualifiedName, attrib);
    }

    // *** columnAttribListByFamilyNameMap
    private Map<String, List<ColumnAttrib>> getColumnAttribListByFamilyNameMap() {
        return this.columnAttribListByFamilyNameMap;
    }

    public Set<String> getFamilySet() {
        return this.getColumnAttribListByFamilyNameMap().keySet();
    }

    public List<ColumnAttrib> getColumnAttribListByFamilyName(final String familyName) {
        return this.getColumnAttribListByFamilyNameMap().get(familyName);
    }

    public boolean containsFamilyNameInFamilyNameMap(final String familyName) {
        return this.getColumnAttribListByFamilyNameMap().containsKey(familyName);
    }

    public void addAttribToFamilyNameColumnListMap(final String familyName,
                                                   final List<ColumnAttrib> attribList) throws HBqlException {
        if (this.containsFamilyNameInFamilyNameMap(familyName))
            throw new HBqlException(familyName + " already declared");
        this.getColumnAttribListByFamilyNameMap().put(familyName, attribList);
    }

    public void addAttribToFamilyNameColumnListMap(ColumnAttrib attrib) throws HBqlException {

        if (attrib.isAKeyAttrib() || attrib.isFamilyDefaultAttrib())
            return;

        final String familyName = attrib.getFamilyName();

        if (familyName == null || familyName.length() == 0)
            return;

        final List<ColumnAttrib> attribList;
        if (!this.containsFamilyNameInFamilyNameMap(familyName)) {
            attribList = Lists.newArrayList();
            this.addAttribToFamilyNameColumnListMap(familyName, attribList);
        }
        else {
            attribList = this.getColumnAttribListByFamilyName(familyName);
        }
        attribList.add(attrib);
    }

    public HBqlFilter getHBqlFilter(final ExpressionTree origExpressionTree) throws HBqlException {

        if (origExpressionTree == null)
            return null;

        origExpressionTree.setSchemaAndContext(this);
        return new HBqlFilter(origExpressionTree);
    }

    public synchronized Set<String> getSchemaFamilyNames(final HConnection connection) throws HBqlException {

        // TODO May not want to cache this
        if (this.familyNameSet == null) {
            // Connction will be null from tests
            this.familyNameSet = (connection == null)
                                 ? this.getFamilySet()
                                 : connection.getFamilyNames(this.getTableName());
        }

        return this.familyNameSet;
    }

    private void processColumn(final ColumnDescription columnDescription,
                               final boolean requireFamilyName) throws HBqlException {

        final HBaseAttrib attrib = new HBaseAttrib(columnDescription);

        this.addAttribToVariableNameMap(attrib, attrib.getNamesForColumn());
        this.addAttribToFamilyQualifiedNameMap(attrib);
        this.addVersionAttrib(attrib);
        this.addFamilyDefaultAttrib(attrib);
        this.addAttribToFamilyNameColumnListMap(attrib);

        if (attrib.isAKeyAttrib()) {
            if (this.getKeyAttrib() != null)
                throw new HBqlException("Schema " + this + " has multiple instance variables marked as keys");
            this.setKeyAttrib(attrib);
        }
        else {
            final String family = attrib.getFamilyName();
            if (requireFamilyName && family.length() == 0)
                throw new HBqlException(attrib.getColumnName() + " is missing family name");
        }
    }

    public HBqlFilter newHBqlFilter(final String query) throws HBqlException {
        final ExpressionTree expressionTree = HBqlShell.parseWhereExpression(query, this);
        return new HBqlFilter(expressionTree);
    }
}
