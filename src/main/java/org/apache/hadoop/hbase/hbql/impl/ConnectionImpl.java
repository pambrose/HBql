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

import org.apache.expreval.util.Sets;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.hbql.client.Batch;
import org.apache.hadoop.hbase.hbql.client.ExecutionOutput;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.hbql.client.Query;
import org.apache.hadoop.hbase.hbql.parser.HBqlShell;
import org.apache.hadoop.hbase.hbql.schema.AnnotationMapping;
import org.apache.hadoop.hbase.hbql.schema.HRecordMapping;
import org.apache.hadoop.hbase.hbql.statement.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Set;

public class ConnectionImpl implements HConnection {

    private final HBaseConfiguration config;
    private final String name;

    public ConnectionImpl(final String name, final HBaseConfiguration config) {
        this.name = name;
        this.config = (config == null) ? new HBaseConfiguration() : config;
    }

    public <T> Query<T> newQuery(final String query) throws IOException, HBqlException {
        return this.newQuery(query, null);
    }

    public <T> Query<T> newQuery(final String query, final Class clazz) throws IOException, HBqlException {
        final AnnotationMapping mapping;
        if (clazz != null) {
            mapping = AnnotationMapping.getAnnotationMapping(clazz);
            if (mapping == null)
                throw new HBqlException("Unknown class " + clazz.getName());
        }
        else {
            mapping = null;
        }

        return new QueryImpl<T>(this, query, mapping);
    }

    public <T> Query<T> newQuery(final SelectStatement selectStatement) throws IOException, HBqlException {
        return new QueryImpl<T>(this, selectStatement, new HRecordMapping(selectStatement.getSchema()));
    }

    public String getName() {
        return this.name;
    }

    public HBaseConfiguration getConfig() {
        return this.config;
    }

    public HBaseAdmin getAdmin() throws MasterNotRunningException {
        return new HBaseAdmin(this.getConfig());
    }

    public HTable getHTable(final String tableName) throws IOException {
        return new HTable(this.getConfig(), tableName);
    }

    public boolean tableExists(final String tableName) throws MasterNotRunningException {
        return this.getAdmin().tableExists(tableName);
    }

    public boolean tableEnabled(final String tableName) throws IOException {
        return this.getAdmin().isTableEnabled(tableName);
    }

    public void dropTable(final String tableName) throws IOException {
        this.getAdmin().deleteTable(tableName);
    }

    public void disableTable(final String tableName) throws IOException {
        this.getAdmin().disableTable(tableName);
    }

    public void enableTable(final String tableName) throws IOException {
        this.getAdmin().enableTable(tableName);
    }

    public Set<String> getTableNames() throws IOException {
        final HBaseAdmin admin = this.getAdmin();
        final Set<String> tableSet = Sets.newHashSet();
        for (final HTableDescriptor table : admin.listTables())
            tableSet.add(table.getNameAsString());
        return tableSet;
    }

    public Set<String> getFamilyNames(final String tableName) throws HBqlException {
        try {
            final HTableDescriptor table = this.getAdmin().getTableDescriptor(Bytes.toBytes(tableName));
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

    public ExecutionOutput execute(final String str) throws HBqlException, IOException {
        final ConnectionStatement statement = HBqlShell.parseConnectionStatement(str);
        return statement.execute(this);
    }

    public PreparedStatement prepare(final String str) throws HBqlException {
        final PreparedStatement stmt = HBqlShell.parsePreparedStatement(str);
        // Need to call this here to enable setParameters
        stmt.validate(this);
        return stmt;
    }

    public void apply(final Batch batch) throws IOException {
        for (final String tableName : batch.getActionList().keySet()) {
            final HTable table = this.getHTable(tableName);
            for (final BatchAction batchAction : batch.getActionList(tableName))
                batchAction.apply(table);
            table.flushCommits();
        }
    }
}
