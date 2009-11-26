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

import org.apache.expreval.util.Maps;
import org.apache.expreval.util.Sets;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HMapping;
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.HStatement;
import org.apache.hadoop.hbase.hbql.mapping.AnnotationResultAccessor;
import org.apache.hadoop.hbase.hbql.mapping.FamilyMapping;
import org.apache.hadoop.hbase.hbql.mapping.HBaseTableMapping;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HConnectionImpl implements HConnection {

    private final String name;
    private final HBaseConfiguration config;
    private boolean closed = false;
    private HBaseAdmin hbaseAdmin = null;

    private final MappingManager mappingManager;
    private final Map<Class, AnnotationResultAccessor> annotationMappingMap = Maps.newHashMap();

    public HConnectionImpl(final String name, final HBaseConfiguration config) throws HBqlException {
        this.name = name;
        this.config = (config == null) ? new HBaseConfiguration() : config;
        this.mappingManager = new MappingManager(this);

        this.getMappingManager().validatePersistentMetadata();
    }

    public String getName() {
        return this.name;
    }

    public HBaseConfiguration getConfig() {
        return this.config;
    }

    private MappingManager getMappingManager() {
        return this.mappingManager;
    }

    private Map<Class, AnnotationResultAccessor> getAnnotationMappingMap() {
        return this.annotationMappingMap;
    }

    public AnnotationResultAccessor getAnnotationMapping(final Object obj) throws HBqlException {
        return this.getAnnotationMapping(obj.getClass());
    }

    public synchronized AnnotationResultAccessor getAnnotationMapping(final Class<?> clazz) throws HBqlException {

        AnnotationResultAccessor accessor = getAnnotationMappingMap().get(clazz);

        if (accessor != null)
            return accessor;

        accessor = AnnotationResultAccessor.newAnnotationMapping(this, clazz);

        getAnnotationMappingMap().put(clazz, accessor);

        return accessor;
    }

    public synchronized HBaseAdmin newHBaseAdmin() throws HBqlException {

        if (this.hbaseAdmin == null) {
            try {
                this.hbaseAdmin = new HBaseAdmin(this.getConfig());
            }
            catch (MasterNotRunningException e) {
                throw new HBqlException(e);
            }
        }

        return this.hbaseAdmin;
    }

    public Set<String> getFamilyNames(final String tableName) throws HBqlException {
        final HTableDescriptor table = this.getHTableDescriptor(tableName);
        final Set<String> familySet = Sets.newHashSet();
        for (final HColumnDescriptor descriptor : table.getColumnFamilies())
            familySet.add(Bytes.toString(descriptor.getName()));
        return familySet;
    }

    public boolean familyExists(final String tableName, final String familyName) throws HBqlException {
        final Set<String> names = this.getFamilyNames(tableName);
        return names.contains(familyName);
    }

    public HStatement createStatement() {
        return new HStatementImpl(this);
    }

    public HPreparedStatement prepareStatement(final String sql) throws HBqlException {
        return new HPreparedStatementImpl(this, sql);
    }

    public void close() throws HBqlException {
        this.closed = true;
    }

    public boolean isClosed() throws HBqlException {
        return this.closed;
    }

    public ExecutionResults execute(final String sql) throws HBqlException {
        final HStatement stmt = this.createStatement();
        return stmt.execute(sql);
    }

    public HResultSet<HRecord> executeQuery(final String sql) throws HBqlException {
        final HStatement stmt = this.createStatement();
        return stmt.executeQuery(sql);
    }

    public <T> HResultSet<T> executeQuery(final String sql, final Class clazz) throws HBqlException {
        final HStatement stmt = this.createStatement();
        return stmt.executeQuery(sql, clazz);
    }

    public List<HRecord> executeQueryAndFetch(final String sql) throws HBqlException {
        final HStatement stmt = this.createStatement();
        return stmt.executeQueryAndFetch(sql);
    }

    public <T> List<T> executeQueryAndFetch(final String sql, final Class clazz) throws HBqlException {
        final HStatement stmt = this.createStatement();
        return stmt.executeQueryAndFetch(sql, clazz);
    }

    public ExecutionResults executeUpdate(final String sql) throws HBqlException {
        final HStatement stmt = this.createStatement();
        return stmt.executeUpdate(sql);
    }

    // Mapping Routines
    public boolean mappingExists(final String mappingName) throws HBqlException {
        return this.getMappingManager().mappingExists(mappingName);
    }

    public HBaseTableMapping getMapping(final String mappingName) throws HBqlException {
        return this.getMappingManager().getMapping(mappingName);
    }

    public boolean dropMapping(final String mappingName) throws HBqlException {
        return this.getMappingManager().dropMapping(mappingName);
    }

    public Set<HMapping> getMappings() throws HBqlException {
        return this.getMappingManager().getMappings();
    }

    public synchronized HBaseTableMapping createMapping(final boolean tempMapping,
                                                        final String mappingName,
                                                        final String tableName,
                                                        final String keyName,
                                                        final List<FamilyMapping> familyList) throws HBqlException {
        return this.getMappingManager().createMapping(tempMapping, mappingName, tableName, keyName, familyList);
    }

    // Table Routines
    public void createTable(final HTableDescriptor tableDesc) throws HBqlException {

        final String tableName = tableDesc.getNameAsString();
        if (this.tableExists(tableName))
            throw new HBqlException("Table already exists: " + tableName);

        try {
            this.newHBaseAdmin().createTable(tableDesc);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public HTable newHTable(final String tableName) throws HBqlException {
        try {
            return new HTable(this.getConfig(), tableName);
        }
        catch (IOException e) {
            throw new HBqlException("Invalid table name: " + tableName);
        }
    }

    public boolean tableExists(final String tableName) throws HBqlException {
        try {
            return this.newHBaseAdmin().tableExists(tableName);
        }
        catch (MasterNotRunningException e) {
            throw new HBqlException(e);
        }
    }

    public HTableDescriptor getHTableDescriptor(final String tableName) throws HBqlException {
        try {
            this.validateTableName(tableName);
            return this.newHBaseAdmin().getTableDescriptor(tableName.getBytes());
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public boolean tableEnabled(final String tableName) throws HBqlException {
        try {
            this.validateTableName(tableName);
            return this.newHBaseAdmin().isTableEnabled(tableName);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public void dropTable(final String tableName) throws HBqlException {
        try {
            validateTableDisabled("drop", tableName);
            final byte[] tableNameBytes = tableName.getBytes();
            this.newHBaseAdmin().deleteTable(tableNameBytes);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public void disableTable(final String tableName) throws HBqlException {
        try {
            if (!this.tableEnabled(tableName))
                throw new HBqlException("Cannot disable disabled table: " + tableName);
            this.newHBaseAdmin().disableTable(tableName);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public void enableTable(final String tableName) throws HBqlException {
        try {
            validateTableDisabled("enable", tableName);
            this.newHBaseAdmin().enableTable(tableName);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public Set<String> getTableNames() throws HBqlException {
        try {
            final HBaseAdmin admin = this.newHBaseAdmin();
            final Set<String> tableSet = Sets.newHashSet();
            for (final HTableDescriptor table : admin.listTables())
                tableSet.add(table.getNameAsString());
            return tableSet;
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public void validateTableName(final String tableName) throws HBqlException {
        if (!this.tableExists(tableName))
            throw new HBqlException("Table not found: " + tableName);
    }

    public void validateTableDisabled(final String action, final String tableName) throws HBqlException {
        if (this.tableEnabled(tableName))
            throw new HBqlException("Cannot " + action + " enabled table: " + tableName);
    }

    public void validateFamilyExists(final String tableName, final String familyName) throws HBqlException {
        if (!this.familyExists(tableName, familyName))
            throw new HBqlException("Family " + familyName + " not present in table " + tableName);
    }
}
