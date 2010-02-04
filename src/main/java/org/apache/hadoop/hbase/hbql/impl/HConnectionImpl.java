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

package org.apache.hadoop.hbase.hbql.impl;

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
import org.apache.hadoop.hbase.hbql.client.AsyncExecutor;
import org.apache.hadoop.hbase.hbql.client.AsyncExecutorManager;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HMapping;
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.HStatement;
import org.apache.hadoop.hbase.hbql.client.QueryExecutorPool;
import org.apache.hadoop.hbase.hbql.client.QueryExecutorPoolManager;
import org.apache.hadoop.hbase.hbql.mapping.AnnotationResultAccessor;
import org.apache.hadoop.hbase.hbql.mapping.FamilyMapping;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;
import org.apache.hadoop.hbase.hbql.statement.args.KeyInfo;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.util.AtomicReferences;
import org.apache.hadoop.hbase.hbql.util.Maps;
import org.apache.hadoop.hbase.hbql.util.PoolableElement;
import org.apache.hadoop.hbase.hbql.util.Sets;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class HConnectionImpl extends PoolableElement<HConnectionImpl> implements HConnection {

    public static final String MAXTABLEREFS = "maxtablerefs";
    public static final String MASTER = "hbase.master";

    private final AtomicBoolean atomicClosed = new AtomicBoolean(false);
    private final HBaseConfiguration hbaseConfiguration;
    private final HTablePool tablePool;
    private final int maxTablePoolReferencesPerTable;
    private final MappingManager mappingManager;

    private final AtomicReference<Map<Class, AnnotationResultAccessor>> atomicAnnoMapping = AtomicReferences.newAtomicReference();
    private final AtomicReference<HBaseAdmin> atomicHbaseAdmin = AtomicReferences.newAtomicReference();
    private final AtomicReference<IndexedTableAdmin> atomicIndexTableAdmin = AtomicReferences.newAtomicReference();

    private String queryExecutorPoolName = null;
    private String asyncExecutorName = null;

    public HConnectionImpl(final HBaseConfiguration hbaseConfiguration,
                           final HConnectionPoolImpl connectionPool,
                           final int maxTablePoolReferencesPerTable) throws HBqlException {
        super(connectionPool);
        this.hbaseConfiguration = (hbaseConfiguration == null) ? new HBaseConfiguration() : hbaseConfiguration;
        this.maxTablePoolReferencesPerTable = maxTablePoolReferencesPerTable;
        this.tablePool = new HTablePool(this.getHBaseConfiguration(), this.getMaxTablePoolReferencesPerTable());
        this.mappingManager = new MappingManager(this);

        this.getMappingManager().validatePersistentMetadata();
    }

    public static HBaseConfiguration getHBaseConfiguration(final String master) {
        final Configuration configuration = new Configuration();
        configuration.set(HConnectionImpl.MASTER, master);
        return new HBaseConfiguration(configuration);
    }

    private AtomicReference<Map<Class, AnnotationResultAccessor>> getAtomicAnnoMapping() {
        return this.atomicAnnoMapping;
    }

    public void resetElement() {

        try {
            this.getAtomicClosed().set(false);
            this.getMappingManager().clear();
        }
        catch (HBqlException e) {
            e.printStackTrace();
        }

        if (this.getAtomicAnnoMapping().get() != null)
            this.getAtomicAnnoMapping().get().clear();

        this.setQueryExecutorPoolName(null);
    }

    public boolean isPooled() {
        return this.getElementPool() != null;
    }

    public HBaseConfiguration getHBaseConfiguration() {
        return this.hbaseConfiguration;
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
        if (this.getAtomicAnnoMapping().get() == null) {
            synchronized (this) {
                if (this.getAtomicAnnoMapping().get() == null) {
                    final Map<Class, AnnotationResultAccessor> newmap = Maps.newConcurrentHashMap();
                    this.getAtomicAnnoMapping().set(newmap);
                }
            }
        }
        return this.getAtomicAnnoMapping().get();
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

    private AtomicReference<HBaseAdmin> getAtomicHbaseAdmin() {
        return this.atomicHbaseAdmin;
    }

    public HBaseAdmin getHBaseAdmin() throws HBqlException {
        this.checkIfClosed();
        if (this.getAtomicHbaseAdmin().get() == null) {
            synchronized (this) {
                if (this.getAtomicHbaseAdmin().get() == null) {
                    try {
                        this.getAtomicHbaseAdmin().set(new HBaseAdmin(this.getHBaseConfiguration()));
                    }
                    catch (MasterNotRunningException e) {
                        throw new HBqlException(e);
                    }
                }
            }
        }
        return this.getAtomicHbaseAdmin().get();
    }

    private AtomicReference<IndexedTableAdmin> getAtomicIndexTableAdmin() {
        return this.atomicIndexTableAdmin;
    }

    public IndexedTableAdmin getIndexTableAdmin() throws HBqlException {
        this.checkIfClosed();
        if (this.getAtomicIndexTableAdmin().get() == null) {
            synchronized (this) {
                if (this.getAtomicIndexTableAdmin().get() == null) {
                    try {
                        this.getAtomicIndexTableAdmin().set(new IndexedTableAdmin(this.getHBaseConfiguration()));
                    }
                    catch (MasterNotRunningException e) {
                        throw new HBqlException(e);
                    }
                }
            }
        }
        return this.getAtomicIndexTableAdmin().get();
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

    public void releaseElement() {
        this.getElementPool().release(this);
    }

    private AtomicBoolean getAtomicClosed() {
        return this.atomicClosed;
    }

    public boolean isClosed() {
        return this.getAtomicClosed().get();
    }

    public synchronized void close() {
        if (!this.isClosed()) {
            this.getAtomicClosed().set(true);
            // If it is a pool conection, just give it back to pool (reset() will be called on release)
            if (this.isPooled())
                this.releaseElement();
        }
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

    public Set<HMapping> getAllMappings() throws HBqlException {
        return this.getMappingManager().getAllMappings();
    }

    public TableMapping createMapping(final boolean tempMapping,
                                      final boolean systemMapping,
                                      final String mappingName,
                                      final String tableName,
                                      final KeyInfo keyInfo,
                                      final List<FamilyMapping> familyList) throws HBqlException {
        return this.getMappingManager().createMapping(tempMapping,
                                                      systemMapping,
                                                      mappingName,
                                                      tableName,
                                                      keyInfo,
                                                      familyList);
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
    public CompletionQueueExecutor getQueryExecutorForConnection() throws HBqlException {

        if (!Utils.isValidString(this.getQueryExecutorPoolName()))
            throw new HBqlException("Connection not assigned a QueryExecutorPool name");

        this.validateQueryExecutorPoolNameExists(this.getQueryExecutorPoolName());

        final QueryExecutorPool pool = QueryExecutorPoolManager.getQueryExecutorPool(this.getQueryExecutorPoolName());
        final CompletionQueueExecutor executorQueue = ((QueryExecutorPoolImpl)pool).take();

        // Reset it prior to handing it out
        executorQueue.resetElement();
        return executorQueue;
    }

    // The value returned from this call must eventually be released.
    public AsyncExecutorImpl getAsyncExecutorForConnection() throws HBqlException {

        if (!Utils.isValidString(this.getAsyncExecutorName()))
            throw new HBqlException("Connection not assigned an AsyncExecutor name");

        this.validateAsyncExecutorNameExists(this.getAsyncExecutorName());

        final AsyncExecutor executor = AsyncExecutorManager.getAsyncExecutor(this.getAsyncExecutorName());

        return ((AsyncExecutorImpl)executor);
    }


    public boolean usesQueryExecutor() {
        return Utils.isValidString(this.getQueryExecutorPoolName());
    }

    public String getQueryExecutorPoolName() {
        return this.queryExecutorPoolName;
    }

    public void setQueryExecutorPoolName(final String poolName) {
        this.queryExecutorPoolName = poolName;
    }

    public String getAsyncExecutorName() {
        return this.asyncExecutorName;
    }

    public void setAsyncExecutorName(final String poolName) {
        this.asyncExecutorName = poolName;
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

    public void validateQueryExecutorPoolNameExists(final String poolName) throws HBqlException {
        if (!QueryExecutorPoolManager.queryExecutorPoolExists(poolName))
            throw new HBqlException("QueryExecutorPool " + poolName + " does not exist.");
    }

    public void validateAsyncExecutorNameExists(final String name) throws HBqlException {
        if (!AsyncExecutorManager.asyncExecutorExists(name))
            throw new HBqlException("AsyncExecutor " + name + " does not exist.");
    }
}
