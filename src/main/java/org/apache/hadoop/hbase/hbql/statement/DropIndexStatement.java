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

import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

public class DropIndexStatement extends TableStatement {

    private final String indexName;

    public DropIndexStatement(final StatementPredicate predicate, final String indexName, final String tableName) {
        super(predicate, tableName);
        this.indexName = indexName;
    }

    protected ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {
        connection.dropIndex(this.getTableName(), this.indexName);
        return new ExecutionResults("Index " + this.getTableName() + " dropped for " + this.getTableName());
    }

    public static String usage() {
        return "DROP INDEX index_name ON table_name [IF boolean_expression]";
    }
}