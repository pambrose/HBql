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
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;
import org.apache.hadoop.hbase.hbql.statement.args.KeyInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MappingManager {

    private final HConnectionImpl connection;
    private final Map<String, TableMapping> userMappingsMap = Maps.newConcurrentHashMap();
    private final Map<String, TableMapping> systemMappingsMap = Maps.newConcurrentHashMap();

    public MappingManager(final HConnectionImpl connection) {
        this.connection = connection;
    }

    public synchronized void clear() {
        this.getUserMappingsMap().clear();
    }

    public void validatePersistentMetadata() throws HBqlException {

        final String sql1 = "CREATE TEMP SYSTEM MAPPING system_mappings(mapping_name KEY, f1(mapping_obj object alias mapping_obj))";
        this.getConnection().execute(sql1);

        final String sql2 = "CREATE TABLE system_mappings (f1(MAX_VERSIONS: 5)) IF NOT tableExists('system_mappings')";
        this.getConnection().execute(sql2);
    }

    private HConnectionImpl getConnection() {
        return this.connection;
    }

    private Map<String, TableMapping> getUserMappingsMap() {
        return this.userMappingsMap;
    }

    private Map<String, TableMapping> getSystemMappingsMap() {
        return this.systemMappingsMap;
    }

    public Set<HMapping> getAllMappings() throws HBqlException {

        final Set<HMapping> names = Sets.newHashSet();
        names.addAll(getUserMappingsMap().values());
        names.addAll(getSystemMappingsMap().values());

        final String sql = "SELECT mapping_obj FROM system_mappings";
        final List<HRecord> recs = this.getConnection().executeQueryAndFetch(sql);
        for (final HRecord rec : recs)
            names.add((TableMapping)rec.getCurrentValue("mapping_obj"));
        return names;
    }

    public boolean mappingExists(final String mappingName) throws HBqlException {

        if (this.getUserMappingsMap().get(mappingName) != null)
            return true;
        else if (this.getSystemMappingsMap().get(mappingName) != null)
            return true;
        else {
            final String sql = "SELECT mapping_name FROM system_mappings WITH KEYS ?";
            final HPreparedStatement stmt = this.getConnection().prepareStatement(sql);
            stmt.setParameter(1, mappingName);
            final List<HRecord> recs = stmt.executeQueryAndFetch();
            return recs.size() > 0;
        }
    }

    public boolean dropMapping(final String mappingName) throws HBqlException {

        if (this.getUserMappingsMap().containsKey(mappingName)) {
            this.getUserMappingsMap().remove(mappingName);
            return true;
        }
        else if (this.getSystemMappingsMap().containsKey(mappingName)) {
            this.getSystemMappingsMap().remove(mappingName);
            return true;
        }
        else {
            final String sql = "DELETE FROM system_mappings WITH KEYS ?";
            final HPreparedStatement stmt = this.getConnection().prepareStatement(sql);
            stmt.setParameter(1, mappingName);
            final int cnt = stmt.executeUpdate().getCount();
            return cnt > 0;
        }
    }

    public synchronized TableMapping createMapping(final boolean isTemp,
                                                   final boolean isSystem,
                                                   final String mappingName,
                                                   final String tableName,
                                                   final KeyInfo keyInfo,
                                                   final List<FamilyMapping> familyMappingList) throws HBqlException {

        if (!mappingName.equals("system_mappings") && this.mappingExists(mappingName))
            throw new HBqlException("Mapping already defined: " + mappingName);

        final TableMapping tableMapping = new TableMapping(this.getConnection(),
                                                           isTemp,
                                                           isSystem,
                                                           mappingName,
                                                           tableName,
                                                           keyInfo,
                                                           familyMappingList);

        if (tableMapping.isTempMapping()) {
            if (!tableMapping.isSystemMapping())
                this.getUserMappingsMap().put(mappingName, tableMapping);
            else
                this.getSystemMappingsMap().put(mappingName, tableMapping);
        }
        else
            this.insertMapping(tableMapping);

        return tableMapping;
    }

    private void insertMapping(final TableMapping tableMapping) throws HBqlException {

        final String sql = "INSERT INTO system_mappings (mapping_name, mapping_obj) VALUES (?, ?)";
        final HPreparedStatement stmt = this.getConnection().prepareStatement(sql);
        stmt.setParameter(1, tableMapping.getMappingName());
        stmt.setParameter(2, tableMapping);
        stmt.execute();
    }

    public TableMapping getMapping(final String mappingName) throws HBqlException {

        if (this.getUserMappingsMap().containsKey(mappingName)) {
            return this.getUserMappingsMap().get(mappingName);
        }
        else if (this.getSystemMappingsMap().containsKey(mappingName)) {
            return this.getSystemMappingsMap().get(mappingName);
        }
        else {
            final String sql = "SELECT mapping_obj FROM system_mappings WITH KEYS ?";
            final HPreparedStatement stmt = this.getConnection().prepareStatement(sql);
            stmt.setParameter(1, mappingName);
            List<HRecord> recs = stmt.executeQueryAndFetch();

            if (recs.size() == 0)
                throw new HBqlException("Mapping not found: " + mappingName);
            else
                return (TableMapping)recs.get(0).getCurrentValue("mapping_obj");
        }
    }
}
