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

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.schema.FamilyDefinition;

import java.util.List;

public class CreateTableStatement extends SimpleStatement implements ConnectionStatement {

    private final String tableName;
    private final List<FamilyDefinition> familyList;

    public CreateTableStatement(final String tableName, List<FamilyDefinition> familyList) {
        this.tableName = tableName;
        this.familyList = familyList;
    }

    public ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {

        final HTableDescriptor tableDesc = new HTableDescriptor(this.tableName);

        for (final FamilyDefinition familyDefintion : this.familyList)
            tableDesc.addFamily(familyDefintion.getColumnDescriptor());

        connection.createTable(tableDesc);

        return new ExecutionResults("Table " + tableDesc.getNameAsString() + " created.");
    }
}
