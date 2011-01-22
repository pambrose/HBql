/*
 * Copyright (c) 2011.  The Apache Software Foundation
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

import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.impl.ColumnNotAllowedException;
import org.apache.hadoop.hbase.hbql.impl.InvalidTypeException;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

public class PredicateTest extends TestSupport {

    static HConnection connection = null;

    @BeforeClass
    public static void beforeClass() throws HBqlException {

        connection = HConnectionManager.newConnection();
    }

    @Test
    public void testPredicate() throws HBqlException {

        ExecutionResults results;

        results = connection.execute("DROP TABLE nosuchtable IF tableexists('nosuchtable')");
        assertTrue(!results.getPredicate());

        results = connection.execute("CREATE TABLE nosuchtable (f1()) IF tableexists('nosuchtable')");
        assertTrue(!results.getPredicate());

        results = connection.execute("CREATE TEMP MAPPING nosuchmapping IF tableexists('nosuchtable')");
        assertTrue(!results.getPredicate());

        results = connection.execute("ALTER TABLE nosuchtable DROP FAMILY foo IF tableexists('nosuchtable')");
        assertTrue(!results.getPredicate());

        results = connection.execute("DISABLE TABLE testtable IF tableexists('testtable')");
        results = connection.execute("DROP TABLE testtable IF tableexists('testtable')");

        results = connection.execute("CREATE TABLE testtable (f1()) IF NOT tableexists('testtable')");

        results = connection.execute("ALTER TABLE testtable ADD FAMILY f1() IF not familyexistsfortable('f1', 'testtable')");
        assertTrue(!results.getPredicate());

        results = connection.execute("DISABLE TABLE testtable");

        results = connection.execute("ALTER TABLE testtable ADD FAMILY f2() IF not familyexistsfortable('f2', 'testtable')");
        assertTrue(results.getPredicate());

        results = connection.execute("ALTER TABLE testtable ALTER FAMILY f1 TO f3() IF not familyexistsfortable('f3', 'testtable')");
        assertTrue(results.getPredicate());

        results = connection.execute("DISABLE TABLE nosuchtable IF tableexists('nosuchtable') AND tableenabled('nosuchtable')");
        assertFalse(results.getPredicate());

        results = connection.execute("ENABLE TABLE testtable");
    }

    @Test
    public void testInvalidPredicate1() throws HBqlException {

        HBqlException hBqlException = null;
        try {
            connection.execute("DROP TABLE nosuchtable IF tableexists(nosuchtable)");
        }
        catch (HBqlException e) {
            e.printStackTrace();
            hBqlException = e;
        }
        assertTrue(hBqlException != null && hBqlException instanceof ColumnNotAllowedException);
    }

    @Test
    public void testInvalidPredicate2() throws HBqlException {

        HBqlException hBqlException = null;
        try {
            connection.execute("DROP TABLE nosuchtable IF tableexists(3)");
        }
        catch (HBqlException e) {
            e.printStackTrace();
            hBqlException = e;
        }
        assertTrue(hBqlException != null && hBqlException instanceof InvalidTypeException);
    }
}