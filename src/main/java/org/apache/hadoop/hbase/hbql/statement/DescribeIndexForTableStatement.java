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
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.util.Bytes;

public class DescribeIndexForTableStatement extends TableStatement implements ConnectionStatement {

    private final String indexName;

    public DescribeIndexForTableStatement(final String indexName, final String tableName) {
        super(null, tableName);
        this.indexName = indexName;
    }

    private String getIndexName() {
        return this.indexName;
    }

    protected ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {
        return this.execute(connection, this.getIndexName(), this.getTableName());
    }

    static ExecutionResults execute(final HConnectionImpl connection,
                                    final String indexName,
                                    final String tableName) throws HBqlException {

        final ExecutionResults retval = new ExecutionResults();

        if (!connection.tableExists(tableName)) {
            retval.out.println("Table not found: " + tableName);
        }
        else {

            final IndexSpecification index = connection.getIndexForTable(indexName, tableName);

            if (index == null) {
                retval.out.println("Index " + indexName + " not found for table " + tableName);
            }
            else {
                retval.out.println("Index: " + index.getIndexId());
                final byte[][] columns = index.getIndexedColumns();
                for (final byte[] column : columns)
                    retval.out.println("Index key: " + Bytes.toString(column));

                final byte[][] otherColumns = index.getAdditionalColumns();
                if (otherColumns.length > 0) {
                    retval.out.println("Additional columns in index: ");
                    for (final byte[] column : otherColumns)
                        retval.out.println("    " + Bytes.toString(column));
                }
                else {
                    retval.out.println("No additional columns in index.");
                }
            }
        }
        retval.out.flush();
        return retval;
    }

    public static String usage() {
        return "DESCRIBE INDEX index_name ON TABLE table_name";
    }
}