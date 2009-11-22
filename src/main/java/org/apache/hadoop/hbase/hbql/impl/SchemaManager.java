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
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HMapping;
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.mapping.FamilyMapping;
import org.apache.hadoop.hbase.hbql.mapping.HBaseMapping;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaManager {

    private final HConnectionImpl connection;
    private final Map<String, HBaseMapping> schemaMap = Maps.newConcurrentHashMap();

    public SchemaManager(final HConnectionImpl connection) {
        this.connection = connection;
    }

    public void validatePersistentMetadata() throws HBqlException {

        final String sql = "CREATE TEMP SCHEMA system_schemas (" +
                           "schema_name KEY, " +
                           "f1 (schema_obj object alias schema_obj))";
        this.getConnection().execute(sql);

        if (!this.getConnection().tableExists("system_schemas"))
            this.getConnection().execute("CREATE TABLE system_schemas (f1)");
    }

    private HConnectionImpl getConnection() {
        return connection;
    }

    private Map<String, HBaseMapping> getSchemaMap() {
        return this.schemaMap;
    }

    public Set<HMapping> getSchemas() throws HBqlException {

        final Set<HMapping> names = Sets.newHashSet();
        names.addAll(getSchemaMap().values());

        final String sql = "SELECT schema_obj FROM system_schemas)";
        final List<HRecord> recs = this.getConnection().executeQueryAndFetch(sql);

        for (final HRecord rec : recs)
            names.add((HBaseMapping)rec.getCurrentValue("schema_obj"));

        return names;
    }

    public boolean schemaExists(final String schemaName) throws HBqlException {

        if (this.getSchemaMap().get(schemaName) != null)
            return true;
        else {
            final String sql = "SELECT schema_name FROM system_schemas WITH KEYS ?)";
            final HPreparedStatement stmt = this.getConnection().prepareStatement(sql);
            stmt.setParameter(1, schemaName);
            final List<HRecord> recs = stmt.executeQueryAndFetch();
            return recs.size() > 0;
        }
    }

    public boolean dropSchema(final String schemaName) throws HBqlException {

        if (this.getSchemaMap().containsKey(schemaName)) {
            this.getSchemaMap().remove(schemaName);
            return true;
        }
        else {
            final String sql = "DELETE FROM system_schemas WITH KEYS ?)";
            final HPreparedStatement stmt = this.getConnection().prepareStatement(sql);
            stmt.setParameter(1, schemaName);
            final int cnt = stmt.executeUpdate().getCount();
            return cnt > 0;
        }
    }

    public synchronized HBaseMapping createSchema(final boolean tempSchema,
                                                  final String schemaName,
                                                  final String tableName,
                                                  final String keyName,
                                                  final List<FamilyMapping> familyMappingList) throws HBqlException {

        if (!schemaName.equals("system_schemas") && this.schemaExists(schemaName))
            throw new HBqlException("Schema already defined: " + schemaName);

        final HBaseMapping mapping = new HBaseMapping(this.getConnection(),
                                                      tempSchema,
                                                      schemaName,
                                                      tableName,
                                                      keyName,
                                                      familyMappingList);

        if (mapping.isTempMapping())
            this.getSchemaMap().put(schemaName, mapping);
        else
            this.insertSchema(mapping);

        return mapping;
    }

    private void insertSchema(final HBaseMapping mapping) throws HBqlException {

        final String sql = "INSERT INTO system_schemas (schema_name, schema_obj) VALUES (?, ?)";
        final HPreparedStatement stmt = this.getConnection().prepareStatement(sql);
        stmt.setParameter(1, mapping.getMappingName());
        stmt.setParameter(2, mapping);
        stmt.execute();
    }

    public HBaseMapping getSchema(final String schemaName) throws HBqlException {

        if (this.getSchemaMap().containsKey(schemaName)) {
            return this.getSchemaMap().get(schemaName);
        }
        else {
            final String sql = "SELECT schema_obj FROM system_schemas WITH KEYS ?)";
            final HPreparedStatement stmt = this.getConnection().prepareStatement(sql);
            stmt.setParameter(1, schemaName);
            List<HRecord> recs = stmt.executeQueryAndFetch();

            if (recs.size() == 0)
                throw new HBqlException("Schema not found: " + schemaName);

            return (HBaseMapping)recs.get(0).getCurrentValue("schema_obj");
        }
    }
}
