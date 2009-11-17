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

package org.apache.hadoop.hbase.hbql.statement;

import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.schema.ColumnDescription;
import org.apache.hadoop.hbase.hbql.schema.HBaseSchema;

import java.util.List;

public class CreateSchemaStatement extends SchemaContext implements ConnectionStatement {

    private final String schemaName;
    private final String tableName;
    private final List<ColumnDescription> columnDescriptionList;

    public CreateSchemaStatement(final String schemaName,
                                 final String tableName,
                                 final List<ColumnDescription> columnDescriptionList) {
        super(schemaName);
        this.schemaName = schemaName;
        this.tableName = (tableName == null || tableName.length() == 0) ? schemaName : tableName;
        this.columnDescriptionList = columnDescriptionList;
    }

    private String getTableName() {
        return this.tableName;
    }

    public String getSchemaName() {
        return this.schemaName;
    }

    private List<ColumnDescription> getColumnDescriptionList() {
        return columnDescriptionList;
    }


    public ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {

        final HBaseSchema schema = SchemaManager.createHBaseSchema(connection,
                                                                   this.getSchemaName(),
                                                                   this.getTableName(),
                                                                   this.getColumnDescriptionList());

        this.setSchema(schema);

        for (final ColumnAttrib attrib : schema.getColumnAttribSet()) {
            if (attrib.getFieldType() == null && !attrib.isFamilyDefaultAttrib())
                throw new HBqlException(schema.getSchemaName() + " attribute "
                                        + attrib.getFamilyQualifiedName() + " has unknown type.");
        }

        return new ExecutionResults("Schema " + schema.getSchemaName() + " defined.");
    }
}