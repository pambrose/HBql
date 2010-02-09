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

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.HStatement;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class IHBaseIndexTest extends TestSupport {

    static HConnection connection = null;

    static Random randomVal = new Random();

    @BeforeClass
    public static void beforeClass() throws HBqlException {

        connection = HConnectionManager.newConnection();

        connection.execute("CREATE TEMP MAPPING tab4 FOR TABLE table22"
                           + "("
                           + "keyval key width 15, "
                           + "f1 ("
                           + "  val1 string alias val1, "
                           + "  val2 int alias val2, "
                           + "  val3 int alias val3, "
                           + "  val4 boolean alias val4 "
                           + "))");

        if (!connection.tableExists("table22"))
            System.out
                    .println(connection.execute("create table table22 (f1(INDEX ON val2 int, INDEX ON val4 boolean))"));
        else {
            System.out.println(connection.execute("delete from tab4"));
        }

        insertRecords(connection, 10);
    }

    private static void insertRecords(final HConnection connection,
                                      final int cnt) throws HBqlException {

        HPreparedStatement stmt = connection.prepareStatement("insert into tab4 " +
                                                              "(keyval, val1, val2, val3, val4) values " +
                                                              "(:key, :val1, :val2, :val3, :val4)");

        for (int i = 0; i < cnt; i++) {

            int val = 10 + i;

            stmt.setParameter("key", Util.getZeroPaddedNonNegativeNumber(i, 15));
            stmt.setParameter("val1", Util.getZeroPaddedNonNegativeNumber(val * 100, 15));
            stmt.setParameter("val2", val);
            stmt.setParameter("val3", randomVal.nextInt());
            stmt.setParameter("val4", (i % 2) == 0);
            stmt.execute();
        }
    }

    private int showValues(final String sql, final boolean expectException) {

        Exception ex = null;
        try {
            HStatement stmt = connection.createStatement();
            HResultSet<HRecord> results = stmt.executeQuery(sql);

            int rec_cnt = 0;
            System.out.println("Results:");

            for (HRecord rec : results) {

                String keyval = (String)rec.getCurrentValue("keyval");
                String val1 = (String)rec.getCurrentValue("val1");
                int val2 = (Integer)rec.getCurrentValue("f1:val2");
                int val3 = (Integer)rec.getCurrentValue("f1:val3");
                boolean val4 = (Boolean)rec.getCurrentValue("f1:val4");

                System.out.println("Current Values: " + keyval + " : " + val1
                                   + " : " + val2 + " : " + val3 + " : " + val4);
                rec_cnt++;
            }

            return rec_cnt;
        }
        catch (HBqlException e) {
            e.printStackTrace();
            ex = e;
        }

        if (expectException)
            assertTrue(ex != null);

        return 0;
    }

    @Test
    public void nonindexSelect1() {
        // Use a key with a bad width
        final String q1 = "select * from tab4 WITH KEY '00000000000001'";
        final int rec_cnt = showValues(q1, true);
        assertTrue(rec_cnt == 0);
    }

    @Test
    public void simpleSelect1() throws HBqlException {

        HStatement stmt = connection.createStatement();

        final String q1 = "select * from tab4";
        final int rec_cnt = showValues(q1, false);
        assertTrue(rec_cnt == 10);
    }

    @Test
    public void simpleSelect2() throws HBqlException {
        final String q1 = "select * from tab4 WITH KEYS ALL";
        final int rec_cnt = showValues(q1, false);
        assertTrue(rec_cnt == 10);
    }

    @Test
    public void simpleSelect3() throws HBqlException {
        final String q1 = "select * from tab4 WITH LIMIT 8";
        final int rec_cnt = showValues(q1, false);
        assertTrue(rec_cnt == 8);
    }

    @Test
    public void simpleSelect3b() throws HBqlException {
        final String q1 = "select * from tab4 WITH KEYS ALL LIMIT 4";
        final int rec_cnt = showValues(q1, false);
        assertTrue(rec_cnt == 4);
    }

    @Test
    public void simpleSelect4() throws HBqlException {
        final String q1 = "select * from tab4 WITH KEYS '000000000000002' TO '000000000000006'";
        final int rec_cnt = showValues(q1, false);
        assertTrue(rec_cnt == 4);
    }

    @Test
    public void simpleSelect5() throws HBqlException {
        final String q1 = "select * from tab4 WITH KEYS FIRST TO '000000000000004'";
        final int rec_cnt = showValues(q1, false);
        assertTrue(rec_cnt == 4);
    }

    @Test
    public void simpleSelect6() throws HBqlException {
        final String q1 = "select * from tab4 WITH KEYS '000000000000005' TO LAST";
        final int rec_cnt = showValues(q1, false);
        assertTrue(rec_cnt == 5);
    }

    @Test
    public void simpleSelect7() throws HBqlException {
        final String q1 = "select * from tab4 WITH KEY '000000000000005' ";
        final int rec_cnt = showValues(q1, false);
        assertTrue(rec_cnt == 1);
    }

    @Test
    public void simpleSelect8() throws HBqlException, IOException {

        final String q1 = "select * from tab4 WITH " +
                          "KEYS '000000000000005' TO LAST " +
                          "SERVER FILTER WHERE val2 = 17 USING INDEX WHERE val2 = 17";

        final int rec_cnt = showValues(q1, false);
        assertTrue(rec_cnt == 1);
    }

    @Test
    public void simpleSelect9() throws HBqlException, IOException {

        final String q1 = "select * from tab4 WITH " +
                          "KEYS '000000000000005' TO LAST " +
                          "SERVER FILTER WHERE val4 = false USING INDEX WHERE val4 = false";

        final int rec_cnt = showValues(q1, false);
        assertTrue(rec_cnt == 3);
    }

    @Test
    public void simpleSelect10() throws HBqlException, IOException {

        final String q1 = "select * from tab4 WITH " +
                          "KEYS '000000000000005' TO LAST " +
                          "SERVER FILTER WHERE val2 = 17 AND val4 = false USING INDEX WHERE val4 = false AND val2 = 17";

        final int rec_cnt = showValues(q1, false);
        assertTrue(rec_cnt == 1);
    }


    @Test
    public void simpleSelect11() throws HBqlException, IOException {

        final String q1 = "select * from tab4 WITH " +
                          "KEYS '000000000000005' TO LAST " +
                          "SERVER FILTER WHERE val2 = 17 AND val4 = false USING INDEX WHERE val4 = false";

        final int rec_cnt = showValues(q1, false);
        assertTrue(rec_cnt == 1);
    }
}