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

import org.apache.expreval.util.ExecutorPool;
import org.apache.expreval.util.GenericExecutor;
import org.apache.expreval.util.Maps;
import org.apache.expreval.util.PoolableElement;
import org.apache.expreval.util.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
import org.apache.hadoop.hbase.client.tableindexed.IndexedTable;
import org.apache.hadoop.hbase.client.tableindexed.IndexedTableAdmin;
import org.apache.hadoop.hbase.client.tableindexed.IndexedTableDescriptor;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HExecutor;
import org.apache.hadoop.hbase.hbql.client.HExecutorPoolManager;
import org.apache.hadoop.hbase.hbql.client.HMapping;
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.HStatement;
import org.apache.hadoop.hbase.hbql.mapping.AnnotationResultAccessor;
import org.apache.hadoop.hbase.hbql.mapping.FamilyMapping;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;
import org.apache.hadoop.hbase.hbql.statement.args.KeyInfo;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HConnectionImpl implements HConnection, PoolableElement {

    public final static String MAXTABLEREFS = "maxtablerefs";
    public final static String MASTER = "hbase.master";

    private final HBaseConfiguration hbaseConfig;
    private final HTablePool tablePool;
    private final HConnectionPoolImpl connectionPool;
    private final int maxTablePoolReferencesPerTable;

    private final MappingManager mappingManager;
    private volatile Map<Class, AnnotationResultAccessor> annotationMappingMap = null;

    private volatile HBaseAdmin hbaseAdmin = null;
    private volatile IndexedTableAdmin indexTableAdmin = null;
    private volatile boolean closed = false;

    private String executorPoolName = null;
    private GenericExecutor executor = null;

    public HConnectionImpl(final HBaseConfiguration hbaseConfig,
                           final HConnectionPoolImpl connectionPool,
                           final int maxTablePoolReferencesPerTable) throws HBqlException {
        this.hbaseConfig = (hbaseConfig == null) ? new HBaseConfiguration() : hbaseConfig;
        this.connectionPool = connectionPool;
        this.maxTablePoolReferencesPerTable = maxTablePoolReferencesPerTable;
        this.tablePool = new HTablePool(this.getHBaseConfiguration(), this.getMaxTablePoolReferencesPerTable());
        this.mappingManager = new MappingManager(this);

        this.getMappingManager().validatePersistentMetadata();
    }

    public static HBaseConfiguration getHBaseConfiguration(final String master) {
        final Configuration c = new Configuration();
        c.set(HConnectionImpl.MASTER, master);
        return new HBaseConfiguration(c);
    }

    public void reset() {
        this.setExecutorPoolName(null);
        this.setExecutor(null);
    }

    public boolean isPooled() {
        return this.getConnectionPool() != null;
    }

    private HConnectionPoolImpl getConnectionPool() {
        return this.connectionPool;
    }

    public HBaseConfiguration getHBaseConfiguration() {
        return this.hbaseConfig;
    }

    private HTablePool getTablePool() {
        return this.tablePool;
    }

    private MappingManager getMappingManager() throws HBqlException {
        this.checkIfClosed();
        return this.mappingManager;
    }

    public int getMaxTablePoolReferencesPerTable() {
        return this.maxTablePoolReferencesPerTable;
    }

    private Map<Class, AnnotationResultAccessor> getAnnotationMappingMap() {
        if (this.annotationMappingMap == null) {
            synchronized (this) {
                if (this.annotationMappingMap == null)
                    this.annotationMappingMap = Maps.newConcurrentHashMap();
            }
        }
        return this.annotationMappingMap;
    }

    public AnnotationResultAccessor getAnnotationMapping(final Object obj) throws HBqlException {
        return this.getAnnotationMapping(obj.getClass());
    }

    public synchronized AnnotationResultAccessor getAnnotationMapping(final Class<?> clazz) throws HBqlException {

        AnnotationResultAccessor accessor = getAnnotationMappingMap().get(clazz);

        if (accessor != null) {
            return accessor;
        }
        else {
            accessor = AnnotationResultAccessor.newAnnotationMapping(this, clazz);
            getAnnotationMappingMap().put(clazz, accessor);
            return accessor;
        }
    }

    public HBaseAdmin getHBaseAdmin() throws HBqlException {
        this.checkIfClosed();
        if (this.hbaseAdmin == null) {
            synchronized (this) {
                if (this.hbaseAdmin == null)
                    try {
                        this.hbaseAdmin = new HBaseAdmin(this.getHBaseConfiguration());
                    }
                    catch (MasterNotRunningException e) {
                        throw new HBqlException(e);
                    }
            }
        }
        return this.hbaseAdmin;
    }

    public IndexedTableAdmin getIndexTableAdmin() throws HBqlException {
        this.checkIfClosed();
        if (this.indexTableAdmin == null) {
            synchronized (this) {
                if (this.indexTableAdmin == null)
                    try {
                        this.indexTableAdmin = new IndexedTableAdmin(this.getHBaseConfiguration());
                    }
                    catch (MasterNotRunningException e) {
                        throw new HBqlException(e);
                    }
            }
        }
        return this.indexTableAdmin;
    }

    public Set<String> getFamilyNames(final String tableName) throws HBqlException {
        this.checkIfClosed();
        final HTableDescriptor table = this.getHTableDescriptor(tableName);
        final Set<String> familySet = Sets.newHashSet();
        for (final HColumnDescriptor descriptor : table.getColumnFamilies())
            familySet.add(Bytes.toString(descriptor.getName()));
        return familySet;
    }

    public boolean familyExistsForTable(final String familyName, final String tableName) throws HBqlException {
        final Set<String> names = this.getFamilyNames(tableName);
        return names.contains(familyName);
    }

    public boolean familyExistsForMapping(final String familyName, final String mappingName) throws HBqlException {
        final TableMapping mapping = this.getMapping(mappingName);
        return mapping.containsFamily(familyName);
    }

    public IndexedTableDescriptor newIndexedTableDescriptor(final String tableName) throws HBqlException {
        this.checkIfClosed();
        try {
            final HTableDescriptor tableDesc = this.getHTableDescriptor(tableName);
            return new IndexedTableDescriptor(tableDesc);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public boolean indexExistsForMapping(final String indexName, final String mappingName) throws HBqlException {
        final TableMapping mapping = this.getMapping(mappingName);
        return this.indexExistsForTable(indexName, mapping.getTableName());
    }

    public IndexSpecification getIndexForTable(final String indexName, final String tableName) throws HBqlException {
        final IndexedTableDescriptor indexDesc = this.newIndexedTableDescriptor(tableName);
        return indexDesc.getIndex(indexName);
    }

    public boolean indexExistsForTable(final String indexName, final String tableName) throws HBqlException {
        this.checkIfClosed();
        final IndexSpecification index = this.getIndexForTable(indexName, tableName);
        return index != null;
    }

    public void dropIndexForMapping(final String indexName, final String mappingName) throws HBqlException {
        final TableMapping mapping = this.getMapping(mappingName);
        this.dropIndexForTable(mapping.getTableName(), indexName);
    }

    public void dropIndexForTable(final String tableName, final String indexName) throws HBqlException {
        this.validateIndexExistsForTable(indexName, tableName);
        try {
            final IndexedTableAdmin ita = this.getIndexTableAdmin();
            ita.removeIndex(tableName.getBytes(), indexName);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public HStatement createStatement() throws HBqlException {
        this.checkIfClosed();
        return new HStatementImpl(this);
    }

    public HPreparedStatement prepareStatement(final String sql) throws HBqlException {
        this.checkIfClosed();
        return new HPreparedStatementImpl(this, sql);
    }

    public void release() {
        this.getConnectionPool().releaseConnection(this);
    }

    public void close() throws HBqlException {
        // If it is a pool conection, just give it back to pool
        if (this.isPooled())
            this.release();
        else
            this.closed = true;
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

    public TableMapping getMapping(final String mappingName) throws HBqlException {
        return this.getMappingManager().getMapping(mappingName);
    }

    public boolean dropMapping(final String mappingName) throws HBqlException {
        return this.getMappingManager().dropMapping(mappingName);
    }

    public Set<HMapping> getMappings() throws HBqlException {
        return this.getMappingManager().getMappings();
    }

    public synchronized TableMapping createMapping(final boolean tempMapping,
                                                   final String mappingName,
                                                   final String tableName,
                                                   final KeyInfo keyInfo,
                                                   final List<FamilyMapping> familyList) throws HBqlException {
        return this.getMappingManager().createMapping(tempMapping, mappingName, tableName, keyInfo, familyList);
    }

    // Table Routines
    public void createTable(final HTableDescriptor tableDesc) throws HBqlException {
        this.checkIfClosed();
        final String tableName = tableDesc.getNameAsString();
        if (this.tableExists(tableName))
            throw new HBqlException("Table already exists: " + tableName);

        try {
            this.getHBaseAdmin().createTable(tableDesc);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public HTableWrapper newHTableWrapper(final WithArgs withArgs, final String tableName) throws HBqlException {

        this.checkIfClosed();

        try {
            if (withArgs != null && withArgs.hasAnIndex())
                return new HTableWrapper(new IndexedTable(this.getHBaseConfiguration(), tableName.getBytes()), null);
            else
                return new HTableWrapper(this.getTablePool().getTable(tableName), this.getTablePool());
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
        catch (RuntimeException e) {
            throw new HBqlException("Invalid table name: " + tableName);
        }
    }

    public boolean tableExists(final String tableName) throws HBqlException {
        try {
            return this.getHBaseAdmin().tableExists(tableName);
        }
        catch (MasterNotRunningException e) {
            throw new HBqlException(e);
        }
    }

    public HTableDescriptor getHTableDescriptor(final String tableName) throws HBqlException {
        this.validateTableName(tableName);
        try {
            return this.getHBaseAdmin().getTableDescriptor(tableName.getBytes());
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public boolean tableEnabled(final String tableName) throws HBqlException {
        this.validateTableName(tableName);
        try {
            return this.getHBaseAdmin().isTableEnabled(tableName);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public void dropTable(final String tableName) throws HBqlException {
        validateTableDisabled(tableName, "drop");
        try {
            final byte[] tableNameBytes = tableName.getBytes();
            this.getHBaseAdmin().deleteTable(tableNameBytes);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public void disableTable(final String tableName) throws HBqlException {
        try {
            if (!this.tableEnabled(tableName))
                throw new HBqlException("Cannot disable disabled table: " + tableName);
            this.getHBaseAdmin().disableTable(tableName);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public void enableTable(final String tableName) throws HBqlException {
        validateTableDisabled(tableName, "enable");
        try {
            this.getHBaseAdmin().enableTable(tableName);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public Set<String> getTableNames() throws HBqlException {
        try {
            final HBaseAdmin admin = this.getHBaseAdmin();
            final Set<String> tableSet = Sets.newHashSet();
            for (final HTableDescriptor table : admin.listTables())
                tableSet.add(table.getNameAsString());
            return tableSet;
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    // The value returned from this call must be eventually released.
    public GenericExecutor getExecutorForConnection() throws HBqlException {
        // If Connection is assigned an Executor, then just return it.  Otherwise, get one from the pool
        final GenericExecutor retval = this.getExecutor() != null
                                       ? this.getExecutor()
                                       : this.takeExecutorFromPool();
        // Reset it prior to handing it out
        retval.reset();
        return retval;
    }

    private GenericExecutor takeExecutorFromPool() throws HBqlException {
        this.validateExecutorPoolNameExists(this.getExecutorPoolName());
        final ExecutorPool pool = HExecutorPoolManager.getExecutorPool(this.getExecutorPoolName());
        return pool.take();
    }

    public boolean usesAnExecutor() {
        return this.getExecutor() != null || Utils.isValidString(this.getExecutorPoolName());
    }

    public String getExecutorPoolName() {
        return this.executorPoolName;
    }

    public void setExecutorPoolName(final String poolName) {
        this.executorPoolName = poolName;
    }

    public void setExecutor(final HExecutor executor) {
        this.executor = (GenericExecutor)executor;
    }

    public GenericExecutor getExecutor() {
        return this.executor;
    }

    public void validateTableName(final String tableName) throws HBqlException {
        if (!this.tableExists(tableName))
            throw new HBqlException("Table not found: " + tableName);
    }

    public void validateTableDisabled(final String tableName, final String action) throws HBqlException {
        if (this.tableEnabled(tableName))
            throw new HBqlException("Cannot " + action + " enabled table: " + tableName);
    }

    public void validateFamilyExistsForTable(final String familyName, final String tableName) throws HBqlException {
        if (!this.familyExistsForTable(familyName, tableName))
            throw new HBqlException("Family " + familyName + " not defined for table " + tableName);
    }

    public void validateIndexExistsForTable(final String indexName, final String tableName) throws HBqlException {
        if (!this.indexExistsForTable(indexName, tableName))
            throw new HBqlException("Index " + indexName + " not defined for table " + tableName);
    }

    public void validateExecutorPoolNameExists(final String poolName) throws HBqlException {
        if (!HExecutorPoolManager.executorPoolExists(poolName))
            throw new HBqlException("Executor pool " + poolName + " does not exist.");
    }
}
