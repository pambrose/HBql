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
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.schema.AnnotationResultMapping;
import org.apache.hadoop.hbase.hbql.schema.HBaseMapping;
import org.apache.hadoop.hbase.hbql.schema.ResultMapping;
import org.apache.hadoop.hbase.hbql.schema.Schema;

public abstract class MappingContext extends SimpleStatement {

    private String schemaName = null;
    private Schema schema = null;
    private ResultMapping mapping = null;

    protected MappingContext(final String schemaName) {
        this.schemaName = schemaName;
    }

    protected MappingContext(final Schema schema) {
        this.setSchema(schema);
    }

    protected synchronized void validateMappingName(final HConnectionImpl connection) throws HBqlException {

        if (this.getSchema() == null) {
            try {
                this.setSchema(connection.getSchema(this.getMappingName()));
            }
            catch (HBqlException e) {
                throw new HBqlException("Unknown schema name: " + this.getMappingName());
            }
        }

        this.validateMatchingNames(this.getMapping());
    }

    protected String getMappingName() {
        return this.schemaName;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
        if (this.getSchema() != null)
            this.schemaName = this.getSchema().getSchemaName();
    }

    public Schema getSchema() {
        return this.schema;
    }

    public ResultMapping getMapping() {
        return this.mapping;
    }

    public void setMapping(final ResultMapping mapping) {
        this.mapping = mapping;
    }

    private void validateMatchingNames(final ResultMapping mapping) throws HBqlException {
        if (mapping != null && mapping instanceof AnnotationResultMapping) {
            final String mappingName = mapping.getSchema().getSchemaName();
            final String selectName = this.getSchema().getSchemaName();
            if (!mappingName.equals(selectName))
                throw new HBqlException("Class " + mappingName + " instead of " + selectName);
        }
    }

    public HBaseMapping getHBaseSchema() throws HBqlException {
        return (HBaseMapping)this.getSchema();
    }
}