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

package org.apache.hadoop.hbase.hbql.statement;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
import org.apache.hadoop.hbase.client.tableindexed.IndexedTableDescriptor;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collection;

public class DescribeTableStatement extends TableStatement {

    public DescribeTableStatement(final String tableName) {
        super(null, tableName);
    }

    protected ExecutionResults execute(final HConnectionImpl conn) throws HBqlException {

        final HTableDescriptor tableDesc = conn.getHTableDescriptor(this.getTableName());

        final ExecutionResults retval = new ExecutionResults();
        retval.out.println("Table name: " + tableDesc.getNameAsString());
        retval.out.println("\nFamilies:");
        for (final HColumnDescriptor columnDesc : tableDesc.getFamilies()) {
            retval.out.println("  " + columnDesc.getNameAsString()
                               + "\n    Max versions: " + columnDesc.getMaxVersions()
                               + "\n    TTL: " + columnDesc.getTimeToLive()
                               + "\n    Block size: " + columnDesc.getBlocksize()
                               + "\n    Compression: " + columnDesc.getCompression().getName()
                               + "\n    Compression type: " + columnDesc.getCompressionType().getName()
                               + "\n    Block cache enabled: " + columnDesc.isBlockCacheEnabled()
                               + "\n    Bloom filter: " + columnDesc.isBloomfilter()
                               + "\n    In memory: " + columnDesc.isInMemory()
                               + "\n");
        }

        final IndexedTableDescriptor indexDesc = conn.newIndexedTableDescriptor(this.getTableName());
        final Collection<IndexSpecification> indexes = indexDesc.getIndexes();
        if (indexes.isEmpty()) {
            retval.out.println("No indexes.");
        }
        else {
            retval.out.println("Indexes:");
            for (final IndexSpecification index : indexes) {
                retval.out.println("  " + index.getIndexId());
                final byte[][] columns = index.getIndexedColumns();
                for (final byte[] column : columns)
                    retval.out.println("    " + Bytes.toString(column));
            }
        }
        retval.out.flush();
        return retval;
    }

    public static String usage() {
        return "DESCRIBE TABLE table_name";
    }
}