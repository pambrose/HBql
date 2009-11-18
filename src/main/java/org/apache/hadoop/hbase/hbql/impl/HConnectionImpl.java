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
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.HSchema;
import org.apache.hadoop.hbase.hbql.client.HStatement;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.schema.AnnotationMapping;
import org.apache.hadoop.hbase.hbql.schema.ColumnDescription;
import org.apache.hadoop.hbase.hbql.schema.HBaseSchema;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HConnectionImpl implements HConnection {

    private final String name;
    private final HBaseConfiguration config;
    private boolean closed = false;
    private HBaseAdmin hbaseAdmin = null;

    private final SchemaManager schemaManager;
    private final Map<Class, AnnotationMapping> annotationMappingMap = Maps.newHashMap();

    public HConnectionImpl(final String name, final HBaseConfiguration config) throws HBqlException {
        this.name = name;
        this.config = (config == null) ? new HBaseConfiguration() : config;
        this.schemaManager = new SchemaManager(this);

        this.getSchemaManager().validatePersistentMetadata();
    }

    public String getName() {
        return this.name;
    }

    public HBaseConfiguration getConfig() {
        return this.config;
    }

    private SchemaManager getSchemaManager() {
        return this.schemaManager;
    }

    private Map<Class, AnnotationMapping> getAnnotationMappingMap() {
        return this.annotationMappingMap;
    }

    public AnnotationMapping getAnnotationMapping(final Object obj) throws HBqlException {
        return this.getAnnotationMapping(obj.getClass());
    }

    public synchronized AnnotationMapping getAnnotationMapping(final Class<?> clazz) throws HBqlException {

        AnnotationMapping mapping = getAnnotationMappingMap().get(clazz);

        if (mapping != null)
            return mapping;

        mapping = AnnotationMapping.newAnnotationMapping(this, clazz);

        getAnnotationMappingMap().put(clazz, mapping);

        return mapping;
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
        try {
            final HTableDescriptor table = this.newHBaseAdmin().getTableDescriptor(Bytes.toBytes(tableName));
            final Set<String> familySet = Sets.newHashSet();
            for (final HColumnDescriptor descriptor : table.getColumnFamilies())
                familySet.add(Bytes.toString(descriptor.getName()));
            return familySet;
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new HBqlException(e.getMessage());
        }
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

    public ExecutionResults executeUpdate(final String sql) throws SQLException {
        final HStatement stmt = this.createStatement();
        return stmt.executeUpdate(sql);
    }

    // Schema Routines
    public boolean schemaExists(final String schemaName) throws HBqlException {
        return this.getSchemaManager().schemaExists(schemaName);
    }

    public HBaseSchema getSchema(final String schemaName) throws HBqlException {
        return this.getSchemaManager().getSchema(schemaName);
    }

    public boolean dropSchema(final String schemaName) throws HBqlException {
        return this.getSchemaManager().dropSchema(schemaName);
    }

    public Set<HSchema> getSchemas() throws HBqlException {
        return this.getSchemaManager().getSchemas();
    }

    public synchronized HBaseSchema createSchema(final boolean tempSchema,
                                                 final String schemaName,
                                                 final String tableName,
                                                 final List<ColumnDescription> colList) throws HBqlException {
        return this.getSchemaManager().createSchema(tempSchema, schemaName, tableName, colList);
    }

    // Table Routines
    public void createTable(final HTableDescriptor tableDesc) throws HBqlException {
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

    public boolean tableEnabled(final String tableName) throws HBqlException {
        try {
            return this.newHBaseAdmin().isTableEnabled(tableName);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public void dropTable(final String tableName) throws HBqlException {
        try {
            this.newHBaseAdmin().deleteTable(tableName);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public void disableTable(final String tableName) throws HBqlException {
        try {
            this.newHBaseAdmin().disableTable(tableName);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public void enableTable(final String tableName) throws HBqlException {
        try {
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
}
