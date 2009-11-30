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
import org.apache.hadoop.hbase.hbql.mapping.HBaseTableMapping;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MappingManager {

    private final HConnectionImpl connection;
    private final Map<String, HBaseTableMapping> mappingMap = Maps.newConcurrentHashMap();

    public MappingManager(final HConnectionImpl connection) {
        this.connection = connection;
    }

    public void validatePersistentMetadata() throws HBqlException {

        String sql = "CREATE TEMP MAPPING system_mappings(mapping_name KEY, f1(mapping_obj object alias mapping_obj))";
        this.getConnection().execute(sql);

        sql = "CREATE TABLE system_mappings (f1(MAX_VERSIONS: 5)) IF NOT tableexists('system_mappings')";
        this.getConnection().execute(sql);
    }

    private HConnectionImpl getConnection() {
        return connection;
    }

    private Map<String, HBaseTableMapping> getMappingMap() {
        return this.mappingMap;
    }

    public Set<HMapping> getMappings() throws HBqlException {

        final Set<HMapping> names = Sets.newHashSet();
        names.addAll(getMappingMap().values());

        final String sql = "SELECT mapping_obj FROM system_mappings)";
        final List<HRecord> recs = this.getConnection().executeQueryAndFetch(sql);

        for (final HRecord rec : recs)
            names.add((HBaseTableMapping)rec.getCurrentValue("mapping_obj"));

        return names;
    }

    public boolean mappingExists(final String mappingName) throws HBqlException {

        if (this.getMappingMap().get(mappingName) != null)
            return true;
        else {
            final String sql = "SELECT mapping_name FROM system_mappings WITH KEYS ?)";
            final HPreparedStatement stmt = this.getConnection().prepareStatement(sql);
            stmt.setParameter(1, mappingName);
            final List<HRecord> recs = stmt.executeQueryAndFetch();
            return recs.size() > 0;
        }
    }

    public boolean dropMapping(final String mappingName) throws HBqlException {

        if (this.getMappingMap().containsKey(mappingName)) {
            this.getMappingMap().remove(mappingName);
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

    public synchronized HBaseTableMapping createMapping(final boolean isTemp,
                                                        final String mappingName,
                                                        final String tableName,
                                                        final String keyName,
                                                        final List<FamilyMapping> familyMappingList) throws HBqlException {

        if (!mappingName.equals("system_mappings") && this.mappingExists(mappingName))
            throw new HBqlException("Mapping already defined: " + mappingName);

        final HBaseTableMapping tableMapping = new HBaseTableMapping(this.getConnection(),
                                                                     isTemp,
                                                                     mappingName,
                                                                     tableName,
                                                                     keyName,
                                                                     familyMappingList);

        if (tableMapping.isTempMapping())
            this.getMappingMap().put(mappingName, tableMapping);
        else
            this.insertMapping(tableMapping);

        return tableMapping;
    }

    private void insertMapping(final HBaseTableMapping tableMapping) throws HBqlException {

        final String sql = "INSERT INTO system_mappings (mapping_name, mapping_obj) VALUES (?, ?)";
        final HPreparedStatement stmt = this.getConnection().prepareStatement(sql);
        stmt.setParameter(1, tableMapping.getMappingName());
        stmt.setParameter(2, tableMapping);
        stmt.execute();
    }

    public HBaseTableMapping getMapping(final String mappingName) throws HBqlException {

        if (this.getMappingMap().containsKey(mappingName)) {
            return this.getMappingMap().get(mappingName);
        }
        else {
            final String sql = "SELECT mapping_obj FROM system_mappings WITH KEYS ?";
            final HPreparedStatement stmt = this.getConnection().prepareStatement(sql);
            stmt.setParameter(1, mappingName);
            List<HRecord> recs = stmt.executeQueryAndFetch();

            if (recs.size() == 0)
                throw new HBqlException("Mapping not found: " + mappingName);

            return (HBaseTableMapping)recs.get(0).getCurrentValue("mapping_obj");
        }
    }
}
