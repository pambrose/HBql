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

import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
import org.apache.hadoop.hbase.client.tableindexed.IndexedTableAdmin;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

import java.io.IOException;

public class CreateIndexStatement extends BasicStatement implements ConnectionStatement {

    private final String indexName;
    private final String tableName;
    private final String indexColumn;

    public CreateIndexStatement(final StatementPredicate predicate,
                                final String indexName,
                                final String tableName,
                                final String indexColumn) {
        super(predicate);
        this.indexName = indexName;
        this.tableName = tableName;
        this.indexColumn = indexColumn;
    }

    protected ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {

        try {
            final IndexSpecification spec = new IndexSpecification(this.indexName, this.indexColumn.getBytes());
            final IndexedTableAdmin ita = connection.getIndexTableAdmin();
            ita.addIndex(this.tableName.getBytes(), spec);

            return new ExecutionResults("Index " + this.indexName + " created for "
                                        + this.tableName + " (" + this.indexColumn + ")");
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public static String usage() {
        return "CREATE INDEX index_name ON table_name (index_column) [IF boolean_expression]";
    }
}