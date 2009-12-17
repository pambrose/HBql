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
import org.apache.hadoop.hbase.hbql.mapping.AnnotationResultAccessor;
import org.apache.hadoop.hbase.hbql.mapping.Mapping;
import org.apache.hadoop.hbase.hbql.mapping.ResultAccessor;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;

public abstract class StatementContext extends MappingStatement {

    private Mapping mapping = null;
    private ResultAccessor resultAccessor = null;

    protected StatementContext(final StatementPredicate predicate, final String mappingName) {
        super(predicate, mappingName);
    }

    protected StatementContext(final StatementPredicate predicate, final Mapping mapping) {
        super(predicate, null);
        this.setMapping(mapping);
    }

    protected synchronized void validateMappingName(final HConnectionImpl connection) throws HBqlException {

        if (this.getMapping() == null) {
            try {
                this.setMapping(connection.getMapping(this.getMappingName()));
            }
            catch (HBqlException e) {
                e.printStackTrace();
                throw new HBqlException("Unknown mapping name: " + this.getMappingName());
            }
        }

        this.validateMatchingNames(this.getResultAccessor());
    }

    public void setMapping(final Mapping mapping) {
        this.mapping = mapping;
        if (this.getMapping() != null)
            this.setMappingName(this.getMapping().getMappingName());
    }

    public Mapping getMapping() {
        return this.mapping;
    }

    public ResultAccessor getResultAccessor() {
        return this.resultAccessor;
    }

    public void setResultAccessor(final ResultAccessor resultAccessor) {
        this.resultAccessor = resultAccessor;
    }

    private void validateMatchingNames(final ResultAccessor accessor) throws HBqlException {
        if (accessor != null && accessor instanceof AnnotationResultAccessor) {
            final String mappingName = accessor.getMapping().getMappingName();
            final String selectName = this.getMapping().getMappingName();
            if (!mappingName.equals(selectName))
                throw new HBqlException("Class " + mappingName + " instead of " + selectName);
        }
    }

    public TableMapping getTableMapping() {
        return (TableMapping)this.getMapping();
    }
}