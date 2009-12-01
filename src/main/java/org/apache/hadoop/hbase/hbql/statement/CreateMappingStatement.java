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
import org.apache.hadoop.hbase.hbql.mapping.AttribMapping;
import org.apache.hadoop.hbase.hbql.mapping.FamilyMapping;
import org.apache.hadoop.hbase.hbql.mapping.HBaseTableMapping;

import java.util.List;
import java.util.Set;

public class CreateMappingStatement extends StatementContext implements ConnectionStatement {

    private final boolean tempMapping;
    private final String mappingName;
    private final String tableName;
    private final String keyName;
    private final List<FamilyMapping> familyMappingList;

    public CreateMappingStatement(final StatementPredicate predicate,
                                  final boolean tempMapping,
                                  final String mappingName,
                                  final String tableName,
                                  final AttribMapping attribMapping) {
        super(predicate, mappingName);
        this.tempMapping = tempMapping;
        this.mappingName = mappingName;
        this.tableName = (tableName == null || tableName.length() == 0) ? mappingName : tableName;
        this.keyName = attribMapping != null ? attribMapping.getKeyName() : null;
        this.familyMappingList = attribMapping != null ? attribMapping.getFamilyMappingList() : null;
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

    protected ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {

        final HBaseTableMapping tableMapping = connection.createMapping(this.isTempMapping(),
                                                                        this.getMappingName(),
                                                                        this.getTableName(),
                                                                        this.getKeyName(),
                                                                        this.getFamilyMappingList());

        this.setMapping(tableMapping);

        tableMapping.validate(tableMapping.getMappingName());

        return new ExecutionResults("Mapping " + tableMapping.getMappingName() + " defined.");
    }

    public static String usage() {
        return "CREATE [TEMP] MAPPING mapping_name [FOR TABLE table_name] [(key_name KEY, family_mapping_list)] [IF boolean_expression]";
    }
}