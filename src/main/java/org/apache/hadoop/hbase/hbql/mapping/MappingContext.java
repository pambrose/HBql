/*
 * Copyright (c) 2010.  The Apache Software Foundation
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

package org.apache.hadoop.hbase.hbql.mapping;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

import java.io.Serializable;

public class MappingContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private String mappingName = null;
    private Mapping mapping = null;
    private ResultAccessor resultAccessor = null;

    public MappingContext() {
    }

    public MappingContext(final Mapping mapping) {
        this.setMapping(mapping);
    }

    public MappingContext(final String mappingName) {
        this.setMappingName(mappingName);
    }

    protected String getMappingName() {
        return mappingName;
    }

    protected void setMappingName(final String mappingName) {
        this.mappingName = mappingName;
    }

    public void setMapping(final Mapping mapping) {
        this.mapping = mapping;
        if (this.getMapping() != null)
            this.setMappingName(this.getMapping().getMappingName());
    }

    public Mapping getMapping() {
        return this.mapping;
    }

    public TableMapping getTableMapping() {
        return (TableMapping)this.getMapping();
    }

    public synchronized void validateMappingName(final HConnectionImpl conn) throws HBqlException {

        if (this.getMapping() == null) {
            try {
                this.setMapping(conn.getMapping(this.getMappingName()));
            }
            catch (HBqlException e) {
                e.printStackTrace();
                throw new HBqlException("Unknown mapping name: " + this.getMappingName());
            }
        }

        this.validateMatchingNames(this.getResultAccessor());
    }

    private void validateMatchingNames(final ResultAccessor accessor) throws HBqlException {
        if (accessor != null && accessor instanceof AnnotationResultAccessor) {
            final String mappingName = accessor.getMapping().getMappingName();
            final String selectName = this.getMapping().getMappingName();
            if (!mappingName.equals(selectName))
                throw new HBqlException("Class " + mappingName + " instead of " + selectName);
        }
    }

    public ResultAccessor getResultAccessor() {
        return this.resultAccessor;
    }

    public void setResultAccessor(final ResultAccessor resultAccessor) {
        this.resultAccessor = resultAccessor;
    }
}