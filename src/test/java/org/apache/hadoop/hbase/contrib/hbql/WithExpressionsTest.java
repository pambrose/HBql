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

package org.apache.hadoop.hbase.contrib.hbql;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.util.TestSupport;
import org.junit.Test;

public class WithExpressionsTest extends TestSupport {

    @Test
    public void keysExpressions() throws HBqlException {
        assertValidInput("WITH KEYS 'aaa' TO 'bbb'");
        assertValidInput("WITH KEYS 'sss' TO LAST");
        assertValidInput("WITH KEYS 'fff' TO 'ggg', 'sss'TO LAST, 'sssd' TO LAST");
    }

    @Test
    public void timeExpressions() throws HBqlException {
        assertValidInput("WITH TIMESTAMP RANGE NOW() TO NOW()");
        assertValidInput("WITH TIMESTAMP RANGE NOW() TO NOW()+DAY(1)");
    }

    @Test
    public void versionExpressions() throws HBqlException {
        assertValidInput("WITH VERSIONS 12");
    }

    @Test
    public void timerangeExpressions() throws HBqlException {
        assertValidInput("WITH TIMESTAMP RANGE NOW() TO NOW()+DAY(1)");
        assertValidInput("WITH TIMESTAMP RANGE NOW() - DAY(1) TO NOW() + DAY(1) + DAY(2)");
        assertValidInput("WITH TIMESTAMP RANGE DATE('10/31/94', 'mm/dd/yy') - DAY(1) TO NOW()+DAY(1) + DAY(2)");
    }

    @Test
    public void clientFilterExpressions() throws HBqlException {
        assertValidInput("WITH CLIENT FILTER WHERE TRUE");
        assertValidInput("WITH CLIENT FILTER WHERE {col1 int} col1 < 4");
        assertValidInput("WITH CLIENT FILTER WHERE {fam1:col1 int} fam1:col1 < 4");
    }

    @Test
    public void serverFilterExpressions() throws HBqlException {
        assertValidInput("WITH SERVER FILTER WHERE TRUE");
        assertValidInput("WITH SERVER FILTER WHERE {col1 int} col1 < 4");
        assertValidInput("WITH SERVER FILTER WHERE {fam1:col1 int alias d} fam1:col1 < 4 OR d > 3");
    }
}