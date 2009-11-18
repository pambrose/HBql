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

package org.apache.hadoop.hbase.hbql.client;

import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.schema.ColumnDescription;
import org.apache.hadoop.hbase.hbql.schema.HBaseSchema;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaManager {

    private final HConnectionImpl connection;
    private final Map<String, HBaseSchema> schemaMap = Maps.newHashMap();

    public SchemaManager(final HConnectionImpl connection) {
        this.connection = connection;
    }

    private HConnectionImpl getConnection() {
        return connection;
    }

    private Map<String, HBaseSchema> getSchemaMap() {
        return this.schemaMap;
    }

    public Set<String> getSchemaNames() {
        return getSchemaMap().keySet();
    }

    public boolean schemaExists(final String schemaName) {
        return this.getSchemaMap().get(schemaName) != null;
    }

    public boolean dropSchema(final String schemaName) {
        if (this.getSchemaMap().containsKey(schemaName)) {
            this.getSchemaMap().remove(schemaName);
            return true;
        }
        else {
            return false;
        }
    }

    public synchronized HBaseSchema createSchema(final String schemaName,
                                                 final String tableName,
                                                 final List<ColumnDescription> colList) throws HBqlException {

        if (this.schemaExists(schemaName))
            throw new HBqlException("Schema " + schemaName + " already defined");

        final HBaseSchema schema = new HBaseSchema(schemaName, tableName, colList);

        this.getSchemaMap().put(schemaName, schema);

        return schema;
    }

    public HBaseSchema getSchema(final String schemaName) throws HBqlException {

        final HBaseSchema schema = this.getSchemaMap().get(schemaName);
        if (schema != null)
            return schema;

        throw new HBqlException("Schema not found: " + schemaName);
    }
}
