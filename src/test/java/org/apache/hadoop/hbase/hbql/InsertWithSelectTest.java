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

import org.apache.hadoop.hbase.hbql.client.ConnectionManager;
import org.apache.hadoop.hbase.hbql.client.ExecutionOutput;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.hbql.client.Query;
import org.apache.hadoop.hbase.hbql.client.Results;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class InsertWithSelectTest extends TestSupport {

    static HConnection conn = null;

    static Random randomVal = new Random();

    @BeforeClass
    public static void onetimeSetup() throws HBqlException, IOException {

        SchemaManager.execute("CREATE SCHEMA tab3 FOR TABLE table3"
                              + "("
                              + "keyval key, "
                              + "f1:val1 string alias val1, "
                              + "f1:val2 int alias val2, "
                              + "f1:val3 int alias val3 DEFAULT 12 "
                              + ")");

        conn = ConnectionManager.newConnection();

        if (!conn.tableExists("table3"))
            System.out.println(conn.execute("create table using tab3"));
        else {
            System.out.println(conn.execute("delete from tab3"));
            //System.out.println(conn.execute("disable table table3"));
            //System.out.println(conn.execute("drop table table3"));
            //System.out.println(conn.execute("create table using tab3"));
        }

        insertRecords(conn, 10);
    }

    private static void insertRecords(final HConnection conn,
                                      final int cnt) throws HBqlException, IOException {

        PreparedStatement stmt = conn.prepare("insert into tab3 " +
                                              "(keyval, val1, val2, val3) values " +
                                              "(:key, :val1, :val2, DEFAULT)");

        for (int i = 0; i < cnt; i++) {

            int val = 10 + i;

            final String keyval = Util.getZeroPaddedNumber(i, TestSupport.keywidth);

            stmt.setParameter("key", keyval);
            stmt.setParameter("val1", "" + val);
            stmt.setParameter("val2", val);
            stmt.execute();
        }
    }

    private static void showValues() throws HBqlException, IOException {

        final String query1 = "SELECT keyval, val1, val2, val3 FROM tab3";

        Query<HRecord> q1 = conn.newQuery(query1);

        Results<HRecord> results = q1.getResults();

        int rec_cnt = 0;
        for (HRecord rec : results) {

            String keyval = (String)rec.getCurrentValue("keyval");
            String val1 = (String)rec.getCurrentValue("val1");
            int val2 = (Integer)rec.getCurrentValue("val2");
            int val3 = (Integer)rec.getCurrentValue("val3");

            System.out.println("Current Values: " + keyval + " : " + val1 + " : " + val2 + " : " + val3);
            rec_cnt++;
        }

        assertTrue(rec_cnt == 10);
    }

    @Test
    public void insertWithSelect() throws HBqlException, IOException {

        final String q1 = "insert into tab3 " +
                          "(keyval, val1, val2) " +
                          "select keyval, val1+val1, val2+1 FROM tab3 ";
        showValues();

        PreparedStatement stmt = conn.prepare(q1);

        ExecutionOutput executionOutput = stmt.execute();

        System.out.println(executionOutput);

        showValues();

        executionOutput = conn.execute(q1);

        System.out.println(executionOutput);

        showValues();
    }

    private Class<? extends Exception> execute(final String str) {

        try {
            conn.execute(str);
        }
        catch (Exception e) {
            e.printStackTrace();
            return e.getClass();
        }
        return null;
    }

    @Test
    public void insertTypeExceptions() throws HBqlException, IOException {

        Class<? extends Exception> caught;

        caught = this.execute("insert into tab3 " +
                              "(keyval, val1, val2) " +
                              "select keyval, DOUBLE(val1+val1), val2+1 FROM tab3 ");
        assertTrue(caught != null && caught == TypeException.class);

        caught = this.execute("insert into tab3 " +
                              "(keyval, val1, val2) " +
                              "select keyval, val2, val1 FROM tab3 ");
        assertTrue(caught != null && caught == TypeException.class);

        caught = this.execute("insert into tab3 " +
                              "(keyval, val1, val2) " +
                              "select keyval, val2 FROM tab3 ");
        assertTrue(caught != null && caught == HBqlException.class);

        caught = this.execute("insert into tab3 " +
                              "(keyval, val1, val2) " +
                              "values ('123', 'aaa', 'ss') ");
        assertTrue(caught != null && caught == TypeException.class);

        caught = this.execute("insert into tab3 " +
                              "(keyval, val1, val2) " +
                              "values (4, 'aaa', 5) ");
        assertTrue(caught != null && caught == TypeException.class);

        caught = this.execute("insert into tab3 " +
                              "(val1, val2) " +
                              "values ('aaa', 5) ");
        assertTrue(caught != null && caught == TypeException.class);

        caught = this.execute("insert into tab3 " +
                              "(keyval, 'd', val2) " +
                              "values (4, 'aaa', 5) ");
        assertTrue(caught != null && caught == TypeException.class);

        caught = this.execute("insert into tab3 " +
                              "(keyval, val1, val2) " +
                              "values (ZEROPAD(12, 5), 'aaa', DEFAULT) ");
        assertTrue(caught != null && caught == HBqlException.class);
    }
}