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

package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;
import java.util.Map;
import java.util.Random;

public class NullValuesTest extends TestSupport {

    static HConnection conn = null;

    static Random randomVal = new Random();

    @BeforeClass
    public static void beforeClass() throws HBqlException {

        conn = HConnectionManager.newConnection();

        conn.execute("CREATE TEMP MAPPING table30"
                     + "("
                     + "keyval key, "
                     + "f1 ("
                     + "  val1 string alias val1, "
                     + "  val2 date alias val2, "
                     + "  val3 int alias val3, "
                     + "  val4 int[] alias val4, "
                     + "  val5 object alias val5, "
                     + "  val6 string alias val6, "
                     + "  val7 int[] alias val7, "
                     + "  val8 string[] alias val8 "
                     + "))");

        if (!conn.tableExists("table30"))
            System.out.println(conn.execute("create table table30 (f1())"));
        else
            System.out.println(conn.execute("delete from table30"));
    }

    public void queryRecords(final int exepected, final String query) throws HBqlException {

        HResultSet<HRecord> resultSet = conn.executeQuery(query);

        System.out.println("Query: " + query);

        int rec_cnt = 0;
        for (HRecord rec : resultSet) {

            String key = (String)rec.getCurrentValue("keyval");
            String val1 = (String)rec.getCurrentValue("val1");
            Date val2 = (Date)rec.getCurrentValue("val2");
            int val3 = (Integer)rec.getCurrentValue("val3");
            int[] val4 = (int[])rec.getCurrentValue("val4");
            Object val5 = rec.getCurrentValue("val5");
            int[] val7 = (int[])rec.getCurrentValue("val7");

            System.out.println("Current Values: " + key
                               + " - " + val1
                               + " - " + val2
                               + " - " + val3
                               + " - " + val4
                               + " - " + val5
                               + " - " + val7[0]
                               + "\n");

            assertTrue(val7[0] == val7_vals[0]);
            assertTrue(val7[1] == val7_vals[1]);
            assertTrue(val7[2] == val7_vals[2]);
            rec_cnt++;
        }

        assertTrue(rec_cnt == exepected);
    }

    public void doQueriesWithClientFilter() throws HBqlException {
        queryRecords(1, "SELECT * FROM table30");
        queryRecords(1, "SELECT * FROM table30 with client filter where null is null");
        queryRecords(0, "SELECT * FROM table30 with client filter where null is not null");
        queryRecords(1, "SELECT * FROM table30 with client filter where val1 is null");
        queryRecords(0, "SELECT * FROM table30 with client filter where val1 is not null");
        queryRecords(0, "SELECT * FROM table30 with client filter where val2 is null");
        queryRecords(1, "SELECT * FROM table30 with client filter where val2 is not null");
        queryRecords(0, "SELECT * FROM table30 with client filter where val3 is null");
        queryRecords(1, "SELECT * FROM table30 with client filter where val3 is not null");
        queryRecords(1, "SELECT * FROM table30 with client filter where val4 is null");
        queryRecords(0, "SELECT * FROM table30 with client filter where val4 is not null");
        queryRecords(1, "SELECT * FROM table30 with client filter where val5 is null");
        queryRecords(0, "SELECT * FROM table30 with client filter where val5 is not null");
        queryRecords(0, "SELECT * FROM table30 with client filter where definedInRow(val6)");
        queryRecords(1, "SELECT * FROM table30 with client filter where not definedInRow(val6)");
    }

    public void doQueriesWithServerFilter() throws HBqlException {
        queryRecords(1, "SELECT * FROM table30");
        queryRecords(1, "SELECT * FROM table30 with server filter where null is null");
        queryRecords(0, "SELECT * FROM table30 with server filter where null is not null");
        queryRecords(1, "SELECT * FROM table30 with server filter where val1 is null");
        queryRecords(0, "SELECT * FROM table30 with server filter where val1 is not null");
        queryRecords(0, "SELECT * FROM table30 with server filter where val2 is null");
        queryRecords(1, "SELECT * FROM table30 with server filter where val2 is not null");
        queryRecords(0, "SELECT * FROM table30 with server filter where val3 is null");
        queryRecords(1, "SELECT * FROM table30 with server filter where val3 is not null");
        queryRecords(1, "SELECT * FROM table30 with server filter where val4 is null");
        queryRecords(0, "SELECT * FROM table30 with server filter where val4 is not null");
        queryRecords(1, "SELECT * FROM table30 with server filter where val5 is null");
        queryRecords(0, "SELECT * FROM table30 with server filter where val5 is not null");
        queryRecords(0, "SELECT * FROM table30 with server filter where definedInRow(val6)");
        queryRecords(1, "SELECT * FROM table30 with server filter where not definedInRow(val6)");
    }

    int[] val7_vals = {1, 2, 3};

    @Test
    public void insert1() throws HBqlException {

        System.out.println(conn.execute("delete from table30"));

        HPreparedStatement stmt = conn.prepareStatement(
                "insert into table30 "
                + "(keyval, val1, val2, val3, val4, val5, val7) values "
                + "(:keyval, :val1, :val2, :val3, :val4, :val5, :val7)");

        final String keyval = Util.getZeroPaddedNonNegativeNumber(1, TestSupport.keywidth);

        stmt.setParameter("keyval", keyval);
        stmt.setParameter("val1", null);
        stmt.setParameter("val2", new Date(System.currentTimeMillis()));
        stmt.setParameter("val3", 0);
        stmt.setParameter("val4", null);
        stmt.setParameter("val5", null);

        stmt.setParameter("val7", val7_vals);
        //stmt.setParameter("val8", null);

        stmt.execute();

        doQueriesWithClientFilter();
        doQueriesWithServerFilter();
    }

    @Test
    public void insert2() throws HBqlException {

        System.out.println(conn.execute("delete from table30"));

        final String keyval = Util.getZeroPaddedNonNegativeNumber(1, TestSupport.keywidth);
        HRecord rec = conn.getMapping("table30").newHRecord();
        rec.setCurrentValue("keyval", keyval);
        rec.setCurrentValue("val1", null);
        rec.setCurrentValue("val2", new Date(System.currentTimeMillis()));
        rec.setCurrentValue("val3", 0);
        rec.setCurrentValue("val4", null);
        rec.setCurrentValue("val5", null);
        rec.setCurrentValue("val7", val7_vals);

        final HBatch<HRecord> batch = conn.newHBatch();
        batch.insert(rec);
        batch.apply();

        doQueriesWithClientFilter();
        doQueriesWithServerFilter();
    }

    @Test
    public void insert3() throws HBqlException {

        System.out.println(conn.execute("delete from table30"));

        final HBatch<HRecord> batch = conn.newHBatch();

        final String keyval = Util.getZeroPaddedNonNegativeNumber(1, TestSupport.keywidth);
        int cnt = 2;
        for (int i = 0; i < cnt; i++) {
            HRecord rec = conn.getMapping("table30").newHRecord();
            rec.setTimestamp(System.currentTimeMillis());
            rec.setCurrentValue("keyval", keyval);
            rec.setCurrentValue("val1", null);
            rec.setCurrentValue("val2", new Date(System.currentTimeMillis()));
            rec.setCurrentValue("val3", 0);
            rec.setCurrentValue("val4", null);
            rec.setCurrentValue("val5", null);
            rec.setCurrentValue("val7", val7_vals);

            batch.insert(rec);

            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        batch.apply();

        final String query = "SELECT * FROM table30 WITH VERSIONS 5";

        HResultSet<HRecord> resultSet = conn.executeQuery(query);

        System.out.println("Query: " + query);

        int rec_cnt = 0;
        for (HRecord rec : resultSet) {

            String key = (String)rec.getCurrentValue("keyval");
            String val1 = (String)rec.getCurrentValue("val1");
            Date val2 = (Date)rec.getCurrentValue("val2");
            int val3 = (Integer)rec.getCurrentValue("val3");
            int[] val4 = (int[])rec.getCurrentValue("val4");
            Object val5 = rec.getCurrentValue("val5");

            Map<Long, Object> val1_history = rec.getVersionMap("val1");
            Map<Long, Object> val2_history = rec.getVersionMap("val2");
            Map<Long, Object> val3_history = rec.getVersionMap("val3");
            Map<Long, Object> val4_history = rec.getVersionMap("val4");
            Map<Long, Object> val5_history = rec.getVersionMap("val5");
            Map<Long, Object> val7_history = rec.getVersionMap("val7");

            if (val1_history.size() == 1)
                System.out.println("Size = " + val1_history.size());
            assertTrue(val1_history.size() == cnt);
            assertTrue(val2_history.size() == cnt);
            assertTrue(val3_history.size() == cnt);
            assertTrue(val4_history.size() == cnt);
            assertTrue(val5_history.size() == cnt);
            assertTrue(val7_history.size() == cnt);

            System.out.println("Current Values: " + key
                               + " - " + val1
                               + " - " + val2
                               + " - " + val3
                               + " - " + val4
                               + " - " + val5
                               + "\n");
            rec_cnt++;
        }

        assertTrue(rec_cnt == 1);
    }
}