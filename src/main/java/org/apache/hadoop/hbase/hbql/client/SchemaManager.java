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
import org.apache.hadoop.hbase.hbql.impl.RecordImpl;
import org.apache.hadoop.hbase.hbql.parser.ParserUtil;
import org.apache.hadoop.hbase.hbql.schema.ColumnDescription;
import org.apache.hadoop.hbase.hbql.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.statement.NonConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.SimpleSchemaContext;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaManager {

    private final static Map<String, HBaseSchema> schemaMap = Maps.newHashMap();

    public static ExecutionResults execute(final String str) throws HBqlException {
        final NonConnectionStatement cmd = ParserUtil.parseSchemaManagerStatement(str);
        return cmd.execute();
    }

    private static Map<String, HBaseSchema> getSchemaMap() {
        return SchemaManager.schemaMap;
    }

    public static Set<String> getHBaseSchemaNames() {
        return getSchemaMap().keySet();
    }

    public static boolean schemaExists(final String schemaName) {
        return null != SchemaManager.getSchemaMap().get(schemaName);
    }

    public static void dropSchema(final String schemaName) {
        if (SchemaManager.getSchemaMap().containsKey(schemaName))
            SchemaManager.getSchemaMap().remove(schemaName);
    }

    public synchronized static HBaseSchema newHBaseSchema(final String schemaName,
                                                          final String tableName,
                                                          final List<ColumnDescription> colList) throws HBqlException {

        if (SchemaManager.schemaExists(schemaName))
            throw new HBqlException("Schema " + schemaName + " already defined");

        final HBaseSchema schema = new HBaseSchema(schemaName, tableName, colList);

        SchemaManager.getSchemaMap().put(schemaName, schema);

        return schema;
    }

    public static HRecord newHRecord(final String schemaName) throws HBqlException {
        final HBaseSchema schema = getSchema(schemaName);
        return new RecordImpl(new SimpleSchemaContext(schema, null));
    }

    public static HBaseSchema getSchema(final String schemaName) throws HBqlException {

        final HBaseSchema schema = SchemaManager.getSchemaMap().get(schemaName);
        if (schema != null)
            return schema;

        throw new HBqlException("Unknown schema: " + schemaName);
    }
}
