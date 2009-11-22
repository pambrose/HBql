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

import org.apache.expreval.util.Sets;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.schema.FamilyMapping;
import org.apache.hadoop.hbase.hbql.schema.HBaseMapping;

import java.util.List;
import java.util.Set;

public class CreateMappingStatement extends MappingContext implements ConnectionStatement {

    private final boolean tempMapping;
    private final String mappingName;
    private final String tableName;
    private final String keyName;
    private final List<FamilyMapping> familyMappingList;

    public CreateMappingStatement(final boolean tempMapping,
                                  final String mappingName,
                                  final String tableName,
                                  final String keyName,
                                  final List<FamilyMapping> familyMappingList) {
        super(mappingName);
        this.tempMapping = tempMapping;
        this.mappingName = mappingName;
        this.tableName = (tableName == null || tableName.length() == 0) ? mappingName : tableName;
        this.keyName = keyName;
        this.familyMappingList = familyMappingList;
    }

    private boolean isTempMapping() {
        return this.tempMapping;
    }

    private String getTableName() {
        return this.tableName;
    }

    public String getMappingName() {
        return this.mappingName;
    }

    public String getKeyName() {
        return this.keyName;
    }

    private List<FamilyMapping> getFamilyMappingList() {
        return familyMappingList;
    }

    public void validate() throws HBqlException {

        // Make sure family names are unique
        if (this.getFamilyMappingList() != null) {
            final Set<String> nameSet = Sets.newHashSet();
            for (final FamilyMapping familyMapping : this.getFamilyMappingList()) {
                final String familyName = familyMapping.getFamilyName();
                if (nameSet.contains(familyName))
                    throw new HBqlException("Family name already mapped: " + familyName);
                else
                    nameSet.add(familyName);
            }
        }
    }

    public ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {

        final HBaseMapping mapping = connection.createMapping(this.isTempMapping(),
                                                              this.getMappingName(),
                                                              this.getTableName(),
                                                              this.getKeyName(),
                                                              this.getFamilyMappingList());

        this.setSchema(mapping);

        for (final ColumnAttrib attrib : mapping.getColumnAttribSet()) {
            if (attrib.getFieldType() == null && !attrib.isFamilyDefaultAttrib())
                throw new HBqlException(mapping.getSchemaName() + " attribute "
                                        + attrib.getFamilyQualifiedName() + " has unknown type.");
        }

        return new ExecutionResults("Mapping " + mapping.getSchemaName() + " defined.");
    }
}