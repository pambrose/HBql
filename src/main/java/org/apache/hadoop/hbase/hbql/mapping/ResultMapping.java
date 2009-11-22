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

package org.apache.hadoop.hbase.hbql.mapping;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.statement.MappingContext;
import org.apache.hadoop.hbase.hbql.statement.select.SelectElement;

import java.io.Serializable;
import java.util.List;

public abstract class ResultMapping implements Serializable {

    private final MappingContext mappingContext;

    public ResultMapping(final MappingContext mappingContext) {
        this.mappingContext = mappingContext;
        this.getMappingContext().setResultMapping(this);
    }

    public MappingContext getMappingContext() {
        return this.mappingContext;
    }

    public Mapping getMapping() {
        return this.getMappingContext().getMapping();
    }

    public HBaseMapping getHBaseMapping() throws HBqlException {
        return (HBaseMapping)this.getMapping();
    }

    public abstract Object newObject(final MappingContext mappingContext,
                                     final List<SelectElement> selectElementList,
                                     final int maxVersions,
                                     final Result result) throws HBqlException;

    public ColumnAttrib getKeyAttrib() throws HBqlException {
        return this.getMapping().getKeyAttrib();
    }

    public abstract ColumnAttrib getColumnAttribByName(String name) throws HBqlException;

    public abstract ColumnAttrib getColumnAttribByQualifiedName(String familyName,
                                                                String columnName) throws HBqlException;
}
