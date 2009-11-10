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

import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.client.Connection;
import org.apache.hadoop.hbase.hbql.client.ConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.hbql.client.Query;
import org.apache.hadoop.hbase.hbql.client.Record;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class AggregateTest extends TestSupport {

    static Connection conn = null;
    static List<String> keyList = Lists.newArrayList();
    static List<String> val1List = Lists.newArrayList();
    static List<Integer> val5List = Lists.newArrayList();
    static int[] val8check = null;
    static int val5max;
    static int val5min;

    static Random randomVal = new Random();

    @BeforeClass
    public static void testSetup() throws HBqlException, IOException {

        SchemaManager.execute("CREATE SCHEMA aggtab FOR TABLE table2"
                              + "("
                              + "keyval key, "
                              + "f1:val1 string alias val1, "
                              + "f1:val2 string alias val2, "
                              + "f1:val3 string alias notdefinedval, "
                              + "f2:val1 date alias val3, "
                              + "f2:val2 date alias val4, "
                              + "f3:val1 int alias val5, "
                              + "f3:val2 int alias val6, "
                              + "f3:val3 int alias val7, "
                              + "f3:val4 int[] alias val8, "
                              + "f3:mapval1 object alias f3mapval1, "
                              + "f3:mapval2 object alias f3mapval2 "
                              + ")");

        conn = ConnectionManager.newConnection();

        if (!conn.tableExists("table2"))
            System.out.println(conn.execute("create table using aggtab"));
        else
            System.out.println(conn.execute("delete from aggtab"));

        insertRecords(conn, 10, "Batch 1");
        insertRecords(conn, 10, "Batch 2");

        keyList.clear();
        val1List.clear();
        val5List.clear();
        val8check = null;

        insertRecords(conn, 10, "Batch 3");

        val5max = Integer.MIN_VALUE;
        for (int i = 0; i < val5List.size(); i++)
            val5max = Math.max(val5List.get(i), val5max);

        val5min = Integer.MAX_VALUE;
        for (int i = 0; i < val5List.size(); i++)
            val5min = Math.min(val5List.get(i), val5min);
    }

    private static void insertRecords(final Connection conn,
                                      final int cnt,
                                      final String msg) throws HBqlException, IOException {

        PreparedStatement stmt = conn.prepare("insert into aggtab " +
                                              "(keyval, val1, val2, val5, val6, f3mapval1, f3mapval2, val8) values " +
                                              "(:key, :val1, :val2, :val5, :val6, :f3mapval1, :f3mapval2, :val8)");

        for (int i = 0; i < cnt; i++) {

            final String keyval = Util.getZeroPaddedNumber(i, TestSupport.keywidth);
            keyList.add(keyval);

            int val5 = randomVal.nextInt();
            String s_val5 = "" + val5;
            val1List.add(s_val5);
            val5List.add(val5);

            Map<String, String> mapval1 = Maps.newHashMap();
            mapval1.put("mapcol1", "mapcol1 val" + i + " " + msg);
            mapval1.put("mapcol2", "mapcol2 val" + i + " " + msg);

            Map<String, String> mapval2 = Maps.newHashMap();
            mapval2.put("mapcol1-b", "mapcol1-b val" + i + " " + msg);
            mapval2.put("mapcol2-b", "mapcol2-b val" + i + " " + msg);
            mapval2.put("mapcol3-b", "mapcol3-b val" + i + " " + msg);

            int[] intv1 = new int[5];
            val8check = new int[5];
            for (int j = 0; j < intv1.length; j++) {
                intv1[j] = j * 10;
                val8check[j] = intv1[j];
            }

            stmt.setParameter("key", keyval);
            stmt.setParameter("val1", s_val5);
            stmt.setParameter("val2", s_val5 + " " + msg);
            stmt.setParameter("val5", val5);
            stmt.setParameter("val6", i * 100);
            stmt.setParameter("f3mapval1", mapval1);
            stmt.setParameter("f3mapval2", mapval2);
            stmt.setParameter("val8", intv1);
            stmt.execute();
        }
    }

    @Test
    public void selectCount() throws HBqlException, IOException {

        final String query1 = "SELECT count() as cnt FROM aggtab";
        Query<Record> q1 = conn.newQuery(query1);
        List<Record> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 1);
        Record rec = recList1.get(0);
        long cnt = (Long)rec.getCurrentValue("cnt");
        assertTrue(cnt == 10);
    }

    @Test
    public void selectMin() throws HBqlException, IOException {

        final String query1 = "SELECT min(val5) as min FROM aggtab";
        Query<Record> q1 = conn.newQuery(query1);
        List<Record> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 1);
        Record rec = recList1.get(0);
        int min = (Integer)rec.getCurrentValue("min");

        assertTrue(min == val5min);
    }
}