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

import org.apache.expreval.expr.ExpressionTree;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.schema.AnnotationMapping;
import org.apache.hadoop.hbase.hbql.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.schema.HRecordMapping;
import org.apache.hadoop.hbase.hbql.schema.Mapping;
import org.apache.hadoop.hbase.hbql.schema.Schema;

import java.io.Serializable;

public abstract class SchemaContext implements HBqlStatement, Serializable {

    private Mapping mapping = null;
    private volatile Schema schema = null;
    private boolean validSchemaName = true;
    private String invalidSchemaName = null;

    protected SchemaContext(final String schemaName) {
        try {
            this.setSchema(SchemaManager.getSchema(schemaName));
        }
        catch (HBqlException e) {
            validSchemaName = false;
            invalidSchemaName = schemaName;
        }
    }

    protected SchemaContext(final Schema schema) {
        this.setSchema(schema);
    }

    protected String getSchemaName() {
        return this.getSchema() == null ? null : this.getSchema().getSchemaName();
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
        this.validSchemaName = true;
    }

    public Schema getSchema() {
        return this.schema;
    }

    public synchronized Mapping getMapping() throws HBqlException {

        // This is the default if it has not already been set.
        if (this.mapping == null)
            this.mapping = new HRecordMapping(this);

        return this.mapping;
    }

    protected void checkIfValidSchemaName() throws HBqlException {
        if (!this.validSchemaName)
            throw new HBqlException("Unknown schema name: " + this.invalidSchemaName);
    }

    public void setMapping(final Mapping mapping) throws HBqlException {
        this.mapping = mapping;
    }

    public synchronized void matchNames(final Mapping mapping) throws HBqlException {

        if (mapping != null && mapping instanceof AnnotationMapping) {
            final String mappingName = mapping.getSchema().getSchemaName();
            final String selectName = this.getSchema().getSchemaName();
            if (!mappingName.equals(selectName))
                throw new HBqlException("Class " + mappingName + " instead of " + selectName);
        }

        this.setMapping(mapping);
    }

    public HBaseSchema getHBaseSchema() throws HBqlException {
        return (HBaseSchema)this.getSchema();
    }

    public HBqlFilter getHBqlFilter(final ExpressionTree origExpressionTree) throws HBqlException {

        if (origExpressionTree == null)
            return null;

        origExpressionTree.setSchemaContext(this);
        return new HBqlFilter(origExpressionTree);
    }
}