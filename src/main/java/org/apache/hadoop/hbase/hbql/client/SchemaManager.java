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
import org.apache.hadoop.hbase.hbql.parser.HBqlShell;
import org.apache.hadoop.hbase.hbql.schema.AnnotationSchema;
import org.apache.hadoop.hbase.hbql.schema.ColumnDescription;
import org.apache.hadoop.hbase.hbql.schema.DefinedSchema;
import org.apache.hadoop.hbase.hbql.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.statement.NonConnectionStatement;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaManager {

    private final static Map<String, DefinedSchema> definedSchemaMap = Maps.newHashMap();

    public static ExecutionOutput execute(final String str) throws HBqlException {
        final NonConnectionStatement cmd = HBqlShell.parseSchemaManagerStatement(str);
        return cmd.execute();
    }

    private static Map<String, DefinedSchema> getDefinedSchemaMap() {
        return SchemaManager.definedSchemaMap;
    }

    public static Set<String> getDefinedSchemaNames() {
        return getDefinedSchemaMap().keySet();
    }

    public static DefinedSchema getDefinedSchema(final String schemaName) {
        return SchemaManager.getDefinedSchemaMap().get(schemaName);
    }

    public static AnnotationSchema getAnnotationSchema(final String schemaName) throws HBqlException {
        return AnnotationSchema.getAnnotationSchema(schemaName);
    }

    public static boolean doesDefinedSchemaExist(final String schemaName) {
        return null != SchemaManager.getDefinedSchemaMap().get(schemaName);
    }

    public static void dropSchema(final String schemaName) {
        if (SchemaManager.getDefinedSchemaMap().containsKey(schemaName))
            SchemaManager.getDefinedSchemaMap().remove(schemaName);
    }

    public synchronized static DefinedSchema newDefinedSchema(final String schemaName,
                                                              final String tableName,
                                                              final List<ColumnDescription> colList) throws HBqlException {

        if (SchemaManager.doesDefinedSchemaExist(schemaName))
            throw new HBqlException("Schema " + schemaName + " already defined");

        final DefinedSchema schema = new DefinedSchema(schemaName, tableName, colList);

        SchemaManager.getDefinedSchemaMap().put(schemaName, schema);

        return schema;
    }

    public static Record newRecord(final String schemaName) throws HBqlException {
        final DefinedSchema schema = getSchema(schemaName).getDefinedSchemaEquivalent();
        return new RecordImpl(schema);
    }

    public static HBaseSchema getSchema(final String schemaName) throws HBqlException {

        // First look in defined schema, then try annotation schema
        HBaseSchema schema;

        schema = getDefinedSchema(schemaName);
        if (schema != null)
            return schema;

        schema = getAnnotationSchema(schemaName);
        if (schema != null)
            return schema;

        throw new HBqlException("Unknown schema: " + schemaName);
    }
}
