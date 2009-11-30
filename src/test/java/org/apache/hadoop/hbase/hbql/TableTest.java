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

package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.junit.Test;

public class TableTest extends TestSupport {

    @Test
    public void simpleTable() throws HBqlException {

        HConnection connection = HConnectionManager.newHConnection();

        assertFalse(connection.tableExists("zzz"));

        String tableName = "tabletest1";
        if (connection.tableExists(tableName)) {
            connection.disableTable(tableName);
            connection.dropTable(tableName);
        }
        assertFalse(connection.tableExists(tableName));
        connection.execute("CREATE TABLE " + tableName + " (family1(), family2(), family3())");
        assertTrue(connection.tableExists(tableName));
        HTableDescriptor table = connection.getHTableDescriptor(tableName);
        HColumnDescriptor[] hcd = table.getColumnFamilies();
        assertTrue((hcd.length == 3));
        assertTrue(table.hasFamily("family1".getBytes()));
        assertTrue(table.hasFamily("family2".getBytes()));
        assertTrue(table.hasFamily("family3".getBytes()));

        assertTrue(table.getNameAsString().equals(tableName));

        connection.disableTable(tableName);
        connection.dropTable(tableName);

        assertFalse(connection.mappingExists(tableName));
    }

    @Test
    public void fullTable() throws HBqlException {

        HConnection connection = HConnectionManager.newHConnection();

        String tableName = "tabletest1";
        if (connection.tableExists(tableName)) {
            connection.disableTable(tableName);
            connection.dropTable(tableName);
        }
        assertFalse(connection.tableExists(tableName));
        connection.execute("CREATE TABLE " + tableName
                           + " (family1 ("
                           + "MAX_VERSIONS:  12, "
                           + "BLOOM_FILTER:  TRUE, "
                           + "BLOCK_SIZE: 123, "
                           + "BLOCK_CACHE_ENABLED: TRUE, "
                           + "COMPRESSION_TYPE: GZ, "
                           + "IN_MEMORY: TRUE, "
                           + "MAP_FILE_INDEX_INTERVAL:  230, "
                           + "TTL: 440"
                           + "))");
        assertTrue(connection.tableExists(tableName));
        HTableDescriptor table = connection.getHTableDescriptor(tableName);
        HColumnDescriptor[] hcd = table.getColumnFamilies();
        assertTrue((hcd.length == 1));
        assertTrue(table.hasFamily("family1".getBytes()));
        assertTrue(table.getNameAsString().equals(tableName));

        HColumnDescriptor family = table.getFamily("family1".getBytes());

        assertTrue(family.getNameAsString().equals("family1"));
        assertTrue(family.getMaxVersions() == 12);
        assertTrue(family.isBloomfilter());
        assertTrue(family.getBlocksize() == 123);
        assertTrue(family.isBlockCacheEnabled());
        assertTrue(family.getCompressionType() == Compression.Algorithm.GZ);
        assertTrue(family.isInMemory());
        assertTrue(family.getValue(HColumnDescriptor.MAPFILE_INDEX_INTERVAL).equals("230"));
        assertTrue(family.getTimeToLive() == 440);

        connection.disableTable(tableName);
        connection.dropTable(tableName);

        assertFalse(connection.mappingExists(tableName));
    }
}