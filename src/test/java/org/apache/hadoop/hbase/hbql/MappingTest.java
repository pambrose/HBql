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

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HMapping;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.Test;

import java.util.Set;

public class MappingTest extends TestSupport {

    @Test
    public void createMapping() throws HBqlException {

        HConnection connection = HConnectionManager.newConnection();

        assertFalse(connection.mappingExists("zzz"));
        assertTrue(connection.mappingExists("system_mappings"));

        String mappingName = "test1";
        connection.dropMapping(mappingName);
        assertFalse(connection.mappingExists(mappingName));
        connection.execute("CREATE MAPPING " + mappingName + " (keyval key, f1 (val2 object alias val3))");
        assertTrue(connection.mappingExists(mappingName));
        HMapping mapping = connection.getMapping(mappingName);
        assertTrue(mapping.getMappingName().equals(mappingName) && mapping.getTableName().equals(mappingName));
        assertTrue(!mapping.isTempMapping());
        connection.dropMapping(mappingName);
        assertFalse(connection.mappingExists(mappingName));

        mappingName = "test2";
        connection.dropMapping(mappingName);
        assertFalse(connection.mappingExists(mappingName));
        connection.execute("CREATE TEMP MAPPING " + mappingName + " (keyval key, f1(val2 object alias val3))");
        assertTrue(connection.mappingExists(mappingName));
        mapping = connection.getMapping(mappingName);
        assertTrue(mapping.getMappingName().equals(mappingName) && mapping.getTableName().equals(mappingName));
        assertTrue(mapping.isTempMapping());
        connection.dropMapping(mappingName);
        assertFalse(connection.mappingExists(mappingName));

        Set<HMapping> mappings = connection.getMappings();
    }
}
