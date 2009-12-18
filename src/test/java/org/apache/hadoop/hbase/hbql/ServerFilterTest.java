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
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.HStatement;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

public class ServerFilterTest extends TestSupport {

    static HConnection connection = null;

    @BeforeClass
    public static void beforeClass() throws HBqlException {

        connection = HConnectionManager.newConnection();

        connection.execute("CREATE TEMP MAPPING tab3 FOR TABLE table20"
                           + "("
                           + "keyval key, "
                           + "f1 ("
                           + "  val1 string alias val1, "
                           + "  val2 int alias val2, "
                           + "  val3 int alias val3 DEFAULT 12 "
                           + "))");

        if (!connection.tableExists("table20"))
            System.out.println(connection.execute("create table table20 (f1())"));
        else {
            System.out.println(connection.execute("delete from tab3"));
        }

        insertRecords(connection, 10);
    }

    private static void insertRecords(final HConnection connection,
                                      final int cnt) throws HBqlException {

        HPreparedStatement stmt = connection.prepareStatement("insert into tab3 " +
                                                              "(keyval, val1, val2, val3) values " +
                                                              "(:key, :val1, :val2, DEFAULT)");

        for (int i = 0; i < cnt; i++) {

            int val = 10 + i;

            final String keyval = Util.getZeroPaddedNonNegativeNumber(i, TestSupport.keywidth);

            stmt.setParameter("key", keyval);
            stmt.setParameter("val1", "" + val);
            stmt.setParameter("val2", val);
            stmt.execute();
        }
    }

    private static void showValues(final String sql, final int cnt) throws HBqlException {

        HStatement stmt = connection.createStatement();
        HResultSet<HRecord> results = stmt.executeQuery(sql);

        int rec_cnt = 0;
        for (HRecord rec : results) {

            String keyval = (String)rec.getCurrentValue("keyval");
            String val1 = (String)rec.getCurrentValue("val1");
            int val2 = (Integer)rec.getCurrentValue("f1:val2");
            int val3 = (Integer)rec.getCurrentValue("val3");

            System.out.println("Current Values: " + keyval + " : " + val1 + " : " + val2 + " : " + val3);
            rec_cnt++;
        }

        assertTrue(rec_cnt == cnt);
    }

    @Test
    public void simpleSelect1() throws HBqlException {
        final String q1 = "select * from tab3";
        showValues(q1, 10);
    }

    @Test
    public void simpleSelect2() throws HBqlException {
        final String q1 = "select * from tab3 WITH SERVER FILTER " +
                          "where val1 = '11' OR val1 = '14' OR  val1 = '12'";
        showValues(q1, 3);
    }

    @Test
    public void simpleSelect3() throws HBqlException {
        final String q1 = "select * from tab3 WITH SERVER FILTER where val1 <= '12' ";
        showValues(q1, 3);
    }

    @Test
    public void simpleSelect4() throws HBqlException {
        final String q1 = "select * from tab3 WITH SERVER FILTER " +
                          //"where val1 = '12' OR val1 = '14' OR val2 = 15";
                          //"where val1 = '11' OR val2 = 14 OR  val1 = '12'";
                          //"where val1 = '11' OR val1 = '14' OR  val1 = '12'";
                          //"where val2 = 14 OR val1 = '11' OR  val1 = '12'";
                          //"where val1 = '11' OR val2 = 14  ";
                          //"where  val2 = 14 OR val1 = '11' ";
                          "where  val2 = 14 ";
        //"where val2 = 14   ";
        showValues(q1, 1);
    }

    @Test
    public void simpleSelect5() throws HBqlException {
        final String q1 = "select * from tab3 WITH SERVER FILTER where val2 BETWEEN 12 AND 14 ";
        showValues(q1, 3);
    }
}