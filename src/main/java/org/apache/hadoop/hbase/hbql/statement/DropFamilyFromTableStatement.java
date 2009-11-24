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

import java.util.List;

public class DropFamilyFromTableStatement extends TableStatement {

    private final List<String> familyNameList;

    public DropFamilyFromTableStatement(final String tableName, final List<String> familyNameList) {
        super(tableName);
        this.familyNameList = familyNameList;
    }

    private List<String> getFamilyNameList() {
        return this.familyNameList;
    }

    public ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {

        final HTableDescriptor tableDescriptor = connection.getHTableDescriptor(this.getTableName());

        for (final String familyName : this.getFamilyNameList())
            tableDescriptor.removeFamily(familyName.getBytes());

        final StringBuilder sbuf = new StringBuilder("Dropped ");
        sbuf.append(this.getFamilyNameList().size() == 1 ? "family " : "families ");
        boolean firsttime = true;
        for (final String familyName : this.getFamilyNameList()) {
            if (!firsttime)
                sbuf.append(", ");
            sbuf.append(familyName);
            firsttime = false;
        }

        sbuf.append(" from table " + this.getTableName());

        return new ExecutionResults(sbuf.toString());
    }
}