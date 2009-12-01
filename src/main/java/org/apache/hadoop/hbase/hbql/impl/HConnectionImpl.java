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

import org.apache.expreval.util.Lists;
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
import org.apache.hadoop.hbase.jdbc.impl.ConnectionImpl;
import org.apache.hadoop.hbase.util.Bytes;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HConnectionImpl implements HConnection, PooledConnection {

    private final HBaseConfiguration config;
    private final HConnectionPoolImpl connectionPool;
    private boolean closed = false;
    private volatile HBaseAdmin hbaseAdmin = null;

    private final MappingManager mappingManager;
    private final Map<Class, AnnotationResultAccessor> annotationMappingMap = Maps.newConcurrentHashMap();

    private volatile List<ConnectionEventListener> connectionEventListenerList = null;
    private volatile List<StatementEventListener> statementEventListenerList = null;

    public HConnectionImpl(final HBaseConfiguration config,
                           final HConnectionPoolImpl connectionPool) throws HBqlException {
        this.config = (config == null) ? new HBaseConfiguration() : config;
        this.connectionPool = connectionPool;
        this.mappingManager = new MappingManager(this);

        this.getMappingManager().validatePersistentMetadata();
    }

    public boolean isPooled() {
        return this.getConnectionPool() != null;
    }

    private HConnectionPoolImpl getConnectionPool() {
        return this.connectionPool;
    }

    public HBaseConfiguration getConfig() {
        return this.config;
    }

    public Connection getConnection() throws SQLException {
        return new ConnectionImpl(this);
    }

    private MappingManager getMappingManager() throws HBqlException {
        this.checkIfClosed();
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

    public HBaseAdmin newHBaseAdmin() throws HBqlException {

        this.checkIfClosed();

        if (this.hbaseAdmin == null) {
            synchronized (this) {
                if (this.hbaseAdmin == null)
                    try {
                        this.hbaseAdmin = new HBaseAdmin(this.getConfig());
                    }
                    catch (MasterNotRunningException e) {
                        throw new HBqlException(e);
                    }
            }
        }

        return this.hbaseAdmin;
    }

    public Set<String> getFamilyNames(final String tableName) throws HBqlException {
        this.checkIfClosed();
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

    public HStatement createStatement() throws HBqlException {
        this.checkIfClosed();
        return new HStatementImpl(this);
    }

    public HPreparedStatement prepareStatement(final String sql) throws HBqlException {
        this.checkIfClosed();
        return new HPreparedStatementImpl(this, sql);
    }

    public void close() throws HBqlException {

        if (this.isPooled())
            this.getConnectionPool().release(this);
        else
            this.closed = true;

        if (this.getRawConnectionEventListenerList() != null) {
            for (final ConnectionEventListener listener : this.getConnectionEventListenerList())
                listener.connectionClosed(new ConnectionEvent(this));
        }
    }

    public boolean isClosed() {
        return this.closed;
    }

    private void checkIfClosed() throws HBqlException {
        if (this.isClosed())
            throw new HBqlException("Connection is closed");
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
        this.checkIfClosed();
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
        this.checkIfClosed();
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

    List<ConnectionEventListener> getRawConnectionEventListenerList() {
        return this.connectionEventListenerList;
    }

    List<ConnectionEventListener> getConnectionEventListenerList() {
        if (this.getRawConnectionEventListenerList() == null)
            synchronized (this) {
                if (this.getRawConnectionEventListenerList() == null)
                    this.connectionEventListenerList = Lists.newArrayList();
            }

        return this.getRawConnectionEventListenerList();
    }

    List<StatementEventListener> getRawStatementEventListenerList() {
        return this.statementEventListenerList;
    }

    List<StatementEventListener> getStatementEventListenerList() {
        if (this.getRawStatementEventListenerList() == null)
            synchronized (this) {
                if (this.getRawStatementEventListenerList() == null)
                    this.statementEventListenerList = Lists.newArrayList();
            }

        return this.getRawStatementEventListenerList();
    }

    public void addConnectionEventListener(final ConnectionEventListener connectionEventListener) {
        this.getConnectionEventListenerList().add(connectionEventListener);
    }

    public void removeConnectionEventListener(final ConnectionEventListener connectionEventListener) {
        this.getConnectionEventListenerList().remove(connectionEventListener);
    }

    public void addStatementEventListener(final StatementEventListener statementEventListener) {
        this.getStatementEventListenerList().add(statementEventListener);
    }

    public void removeStatementEventListener(final StatementEventListener statementEventListener) {
        this.getStatementEventListenerList().remove(statementEventListener);
    }
}
