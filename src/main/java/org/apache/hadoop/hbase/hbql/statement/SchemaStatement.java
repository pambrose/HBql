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

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.schema.HBaseSchema;

public abstract class SchemaStatement implements ShellStatement {

    private final String schemaName;
    private volatile HBaseSchema schema = null;

    protected SchemaStatement(final String schemaName) {
        this.schemaName = schemaName;
    }

    protected final String getSchemaName() {
        return schemaName;
    }

    public final HBaseSchema getSchema() throws HBqlException {

        if (this.schema == null) {
            synchronized (this) {
                if (this.schema == null)
                    this.schema = SchemaManager.getSchema(this.getSchemaName());
            }
        }
        return this.schema;
    }
}