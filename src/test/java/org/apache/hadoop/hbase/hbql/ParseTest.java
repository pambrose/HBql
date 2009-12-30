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
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.statement.HBqlStatement;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.Test;

import java.util.Random;

public class ParseTest extends TestSupport {

    static Random randomVal = new Random();


    public static void parseSQL(final String sql, final int reps, final long maxTime) throws HBqlException {

        long start = System.currentTimeMillis();

        for (int i = 0; i < reps; i++) {
            HBqlStatement stmt = Utils.parseJdbcStatement(sql);

            assertTrue(stmt != null);
        }

        long end = System.currentTimeMillis();

        assertTrue((end - start) < maxTime);
    }

    @Test
    public void parseTest1() throws HBqlException {

        parseSQL("CREATE TEMP MAPPING tab2 FOR TABLE table2"
                 + "("
                 + "keyval key, "
                 + "f1 ("
                 + "  val1 string alias val1, "
                 + "  val2 string alias val2, "
                 + "  val3 string alias notdefinedval "
                 + "), "
                 + "f2 ("
                 + "  val1 date alias val3, "
                 + "  val2 date alias val4 "
                 + "), "
                 + "f3 ("
                 + "  val1 int alias val5, "
                 + "  val2 int alias val6, "
                 + "  val3 int alias val7, "
                 + "  val4 int[] alias val8, "
                 + "  mapval1 object alias f3mapval1, "
                 + "  mapval2 object alias f3mapval2 "
                 + "))", 1000, 1000);
    }

    @Test
    public void parseTest2() throws HBqlException {
        parseSQL("CREATE EXECUTOR POOL pool1", 1000, 1000);
    }

    @Test
    public void parseTest3() throws HBqlException {
        parseSQL("CREATE EXECUTOR POOL pool1 (MAX_POOL_SIZE: 100, THREAD_COUNT: 20, THREADS_READ_RESULTS: TRUE, QUEUE_SIZE: 12 ) ", 500, 1000);
    }
}