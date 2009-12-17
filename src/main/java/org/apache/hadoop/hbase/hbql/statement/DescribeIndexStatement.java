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
import org.apache.hadoop.hbase.client.tableindexed.IndexedTableDescriptor;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.mapping.Mapping;
import org.apache.hadoop.hbase.util.Bytes;

public class DescribeIndexStatement extends BasicStatement implements ConnectionStatement {

    private final String indexName;
    private final String mappingName;

    public DescribeIndexStatement(final String indexName, final String mappingName) {
        super(null);
        this.indexName = indexName;
        this.mappingName = mappingName;
    }

    private String getIndexName() {
        return this.indexName;
    }

    private String getMappingName() {
        return this.mappingName;
    }

    protected ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {

        final Mapping mapping = connection.getMapping(this.getMappingName());

        final ExecutionResults retval = new ExecutionResults();
        final IndexedTableDescriptor indexDesc = connection.newIndexedTableDescriptor(mapping.getTableName());
        final IndexSpecification index = indexDesc.getIndex(this.getIndexName());

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

        retval.out.flush();
        return retval;
    }

    public static String usage() {
        return "DESCRIBE INDEX index_name ON [MAPPING] mapping_name";
    }
}