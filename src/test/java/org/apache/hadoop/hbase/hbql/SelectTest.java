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
import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.HStatement;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

public class SelectTest extends TestSupport {

    static HConnection connection = null;
    static List<String> keyList = Lists.newArrayList();
    static List<String> val1List = Lists.newArrayList();
    static List<Integer> val5List = Lists.newArrayList();
    static int[] val8check = null;

    static Random randomVal = new Random();

    @BeforeClass
    public static void beforeClass() throws HBqlException {

        connection = HConnectionManager.newConnection();

        connection.execute("CREATE TEMP MAPPING tab8 FOR TABLE table1"
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
                           + "))");

        if (!connection.tableExists("table1"))
            System.out.println(connection.execute("create table table1 (f1(), f2(), f3())"));
        else
            System.out.println(connection.execute("delete from tab8"));

        insertRecords(connection, 10, "Batch 1");
        insertRecords(connection, 10, "Batch 2");

        keyList.clear();
        val1List.clear();
        val5List.clear();
        val8check = null;

        insertRecords(connection, 10, "Batch 3");
    }

    private static void insertRecords(final HConnection connection,
                                      final int cnt,
                                      final String msg) throws HBqlException {

        final HBatch<HRecord> batch = HBatch.newHBatch(connection);

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

            final HRecord rec = connection.getMapping("tab8").newHRecord();
            rec.setCurrentValue("keyval", keyval);
            rec.setCurrentValue("val1", s_val5);
            rec.setCurrentValue("val2", s_val5 + " " + msg);
            rec.setCurrentValue("val5", val5);
            rec.setCurrentValue("val6", i * 100);
            rec.setCurrentValue("f3mapval1", mapval1);
            rec.setCurrentValue("f3mapval2", mapval2);
            rec.setCurrentValue("val8", intv1);

            batch.insert(rec);
        }

        batch.apply();
    }


    @Test
    public void selectExpressions() throws HBqlException {

        final String query1 = "SELECT val1, val5, (val5 - val5 + val5) as val6, (val5+val5) as val7 FROM tab8";

        HStatement stmt = connection.createStatement();
        HResultSet<HRecord> results1 = stmt.executeQuery(query1);

        List<String> testKeyVals = Lists.newArrayList();
        List<String> testVal1Vals = Lists.newArrayList();
        List<Integer> testVal5Vals = Lists.newArrayList();
        List<Integer> testVal6Vals = Lists.newArrayList();

        int rec_cnt = 0;
        for (HRecord rec : results1) {

            String keyval = (String)rec.getCurrentValue("keyval");
            String val1 = (String)rec.getCurrentValue("val1");
            Integer val5 = (Integer)rec.getCurrentValue("val5");
            Integer val6 = (Integer)rec.getCurrentValue("val6");

            testKeyVals.add(keyval);
            testVal1Vals.add(val1);
            testVal5Vals.add(val5);
            testVal6Vals.add(val6);

            System.out.println("Current Values: " + keyval
                               + " - " + rec.getCurrentValue("val1")
                               + " - " + rec.getCurrentValue("val5")
                               + " - " + rec.getCurrentValue("val6")
            );
            rec_cnt++;
        }

        assertTrue(testKeyVals.equals(keyList));
        assertTrue(testVal1Vals.equals(val1List));
        assertTrue(testVal5Vals.equals(val5List));
        assertTrue(testVal6Vals.equals(val5List));

        stmt = connection.createStatement();
        List<HRecord> recList2 = stmt.executeQueryAndFetch(query1);

        assertTrue(recList2.size() == rec_cnt);

        final String query3 = "SELECT val1, val5, (val5 - val5 + val5) as val6, (val5+val5) as val7 FROM tab8 " +
                              "WITH KEYS '0000000001' , '0000000002'";

        stmt = connection.createStatement();
        List<HRecord> recList3 = stmt.executeQueryAndFetch(query3);
        assertTrue(recList3.size() == 2);

        final String query4 = "SELECT val1, val5, (val5 - val5 + val5) as val6, (val5+val5) as val7 FROM tab8 " +
                              "WITH KEYS :key1";

        HPreparedStatement pstmt = connection.prepareStatement(query4);

        pstmt.setParameter("key1", "0000000001");
        List<HRecord> recList4 = pstmt.executeQueryAndFetch();

        assertTrue(recList4.size() == 1);

        final String query5 = "SELECT val1, val5, (val5 - val5 + val5) as val6, (val5+val5) as val7 FROM tab8 " +
                              "WITH KEYS :key1, :key2";

        pstmt = connection.prepareStatement(query5);
        pstmt.setParameter("key1", "0000000001");
        pstmt.setParameter("key2", "0000000002");
        List<HRecord> recList5 = pstmt.executeQueryAndFetch();

        assertTrue(recList5.size() == 2);

        final String query6 = "SELECT val1, val5, (val5 - val5 + val5) as val6, (val5+val5) as val7 FROM tab8 " +
                              "WITH KEYS :key1";
        pstmt = connection.prepareStatement(query6);

        List<String> listOfKeys = Lists.newArrayList();
        listOfKeys.add("0000000001");
        listOfKeys.add("0000000002");
        listOfKeys.add("0000000003");
        pstmt.setParameter("key1", listOfKeys);

        List<HRecord> recList6 = pstmt.executeQueryAndFetch();
        assertTrue(recList6.size() == 3);
    }

    @Test
    public void selectMapExpressions() throws HBqlException {

        final String query1 = "SELECT f3mapval1 FROM tab8";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);
        assertTrue(recList1.size() == 10);

        final String query2 = "SELECT f3mapval1, f3mapval2 FROM tab8";
        List<HRecord> recList2 = connection.executeQueryAndFetch(query2);
        assertTrue(recList2.size() == 10);
    }

    @Test
    public void selectVectorExpressions() throws HBqlException {

        final String query1 = "SELECT val8 FROM tab8";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);
        assertTrue(recList1.size() == 10);

        for (final HRecord rec : recList1) {
            int[] intv = (int[])rec.getCurrentValue("val8");
            assertTrue(intv.length == 5);
        }
    }

    @Test
    public void selectInvalidColumnReferences() throws HBqlException {

        final String query1 = "SELECT * FROM tab8 with client FILTER where notdefinedval = 'dd'";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);
        assertTrue(recList1.size() == 0);

        final String query2 = "SELECT * FROM tab8 with client FILTER where DEFINEDINROW(notdefinedval)";
        List<HRecord> recList2 = connection.executeQueryAndFetch(query2);
        assertTrue(recList2.size() == 0);

        final String query3 = "SELECT * FROM tab8 with client FILTER where NOT DEFINEDINROW(notdefinedval)";
        List<HRecord> recList3 = connection.executeQueryAndFetch(query3);
        assertTrue(recList3.size() == 10);

        final String query4 = "SELECT * FROM tab8 with client FILTER where DEFINEDINROW(f1:val1)";
        List<HRecord> recList4 = connection.executeQueryAndFetch(query4);
        assertTrue(recList4.size() == 10);

        final String query5 = "SELECT * FROM tab8 with client FILTER where NOT DEFINEDINROW(f1:val1)";
        List<HRecord> recList5 = connection.executeQueryAndFetch(query5);
        assertTrue(recList5.size() == 0);
    }

    @Test
    public void selectVectorVersionExpressions() throws HBqlException {

        final String query1 = "SELECT f1:val2, val8 FROM tab8 WITH VERSIONS 5";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);
        assertTrue(recList1.size() == 10);

        for (final HRecord rec : recList1) {
            Map<Long, Object> m1 = rec.getVersionMap("f1:val2");
            assertTrue(m1.size() == 3);

            Map<Long, Object> m2 = rec.getVersionMap("val8");
            assertTrue(m2.size() == 3);

            for (Object obj : m2.values()) {
                int[] val8 = (int[])obj;
                for (int i = 0; i < val8.length; i++)
                    assertTrue(val8[i] == val8check[i]);
            }
        }
    }

    @Test
    public void selectFamiliesExpressions() throws HBqlException {

        connection.execute("DROP MAPPING tab8");

        connection.execute("CREATE TEMP MAPPING tab8 FOR TABLE table1"
                           + "("
                           + "keyval key, "
                           + "f1 INCLUDE UNMAPPED ("
                           + "  val2 string alias val2 "
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
                           + "  mapval1 string alias f3mapval1, "
                           + "  mapval2 string alias f3mapval2 "
                           + "))");

        List<HRecord> recList1 = connection.executeQueryAndFetch("SELECT f1:* FROM tab8");
        assertTrue(recList1.size() == 10);

        List<HRecord> recList2 = connection.executeQueryAndFetch("SELECT f1:* FROM tab8 WITH VERSIONS 5");
        assertTrue(recList2.size() == 10);

        List<HRecord> recList3 = connection.executeQueryAndFetch("SELECT * FROM tab8");
        assertTrue(recList3.size() == 10);

        List<HRecord> recList4 = connection.executeQueryAndFetch("SELECT * FROM tab8 WITH VERSIONS 5");
        assertTrue(recList4.size() == 10);

        for (final HRecord rec : recList4) {
            Map<Long, Object> m1 = rec.getVersionMap("val2");
            assertTrue(m1.size() == 3);

            Map<Long, Object> m2 = rec.getVersionMap("val8");
            assertTrue(m2.size() == 3);

            for (Object obj : m2.values()) {
                int[] val8 = (int[])obj;
                for (int i = 0; i < val8.length; i++)
                    assertTrue(val8[i] == val8check[i]);
            }
        }
    }

    @Test
    public void selectUndefinedExpressions() throws HBqlException {

        connection.execute("DROP MAPPING tab8");

        connection.execute("CREATE TEMP MAPPING tab8 FOR TABLE table1"
                           + "("
                           + "keyval key, "
                           + "f1 INCLUDE UNMAPPED "
                           + ")");

        final String query1 = "SELECT f1:val1, f1:val2 FROM tab8";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);

        assertTrue(recList1.size() == 10);

        int i = 0;
        for (final HRecord rec : recList1) {
            Map<String, byte[]> vals = rec.getUnMappedValueMap("f1");
            assertTrue(vals.size() == 2);
            String val1 = IO.getSerialization().getStringFromBytes(vals.get("f1:val1"));
            assertTrue(val1List.get(i).equals(val1));
            i++;
        }
    }

    @Test
    public void selectUndefinedVersionExpressions() throws HBqlException {

        connection.execute("DROP MAPPING tab8");

        connection.execute("CREATE TEMP MAPPING tab8 FOR TABLE table1"
                           + "("
                           + "keyval key, "
                           + "f1 INCLUDE UNMAPPED "
                           + ")");

        final String query1 = "SELECT f1:val1, f1:val2 FROM tab8 WITH VERSIONS 5";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);
        assertTrue(recList1.size() == 10);

        int i = 0;
        for (final HRecord rec : recList1) {
            Map<String, byte[]> vals = rec.getUnMappedValueMap("f1");
            assertTrue(vals.size() == 2);
            String val1 = IO.getSerialization().getStringFromBytes(vals.get("f1:val1"));
            assertTrue(val1List.get(i).equals(val1));

            Map<String, NavigableMap<Long, byte[]>> vers = rec.getUnMappedVersionMap("f1");
            assertTrue(vers.size() == 2);
            NavigableMap<Long, byte[]> v2 = vers.get("f1:val1");
            assertTrue(v2.size() == 3);
            i++;
        }
    }

    @Test
    public void selectUnknownExpressions() throws HBqlException {

        connection.execute("DROP MAPPING tab8");

        connection.execute("CREATE TEMP MAPPING tab8 FOR TABLE table1"
                           + "("
                           + "keyval key, "
                           + "f1 INCLUDE UNMAPPED "
                           + ")");

        final String query1 = "SELECT f1:unknown FROM tab8";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);
        assertTrue(recList1.size() == 10);

        int i = 0;
        for (final HRecord rec : recList1) {
            Map<String, byte[]> vals = rec.getUnMappedValueMap("f1");
            assertTrue(vals.size() == 1);
            String val1 = IO.getSerialization().getStringFromBytes(vals.get("f1:unknown"));
            assertTrue(val1 == null);
            i++;
        }
    }

    @Test
    public void selectUnknownCalcExpressions() throws HBqlException {

        connection.execute("DROP MAPPING tab8");

        connection.execute("CREATE TEMP MAPPING tab8 FOR TABLE table1"
                           + "("
                           + "keyval key, "
                           + "f1 INCLUDE UNMAPPED "
                           + ")");

        final String query1 = "SELECT ('dd'+'ff') as val1 FROM tab8";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);
        assertTrue(recList1.size() == 10);

        int i = 0;
        for (final HRecord rec : recList1) {
            String val = (String)rec.getCurrentValue("val1");
            assertTrue(val.equals("ddff"));

            Map<Long, Object> versions = rec.getVersionMap("val1");
            assertTrue(versions == null || versions.size() == 0);

            i++;
        }
    }

    @Test
    public void selectUnknownMapExpressions() throws HBqlException {

        connection.execute("DROP MAPPING table1");

        connection.execute("CREATE TEMP MAPPING table1"
                           + "("
                           + "keyval key, "
                           + "f3 INCLUDE UNMAPPED "
                           + ")");

        final String query1 = "SELECT f3:* FROM table1";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);
        assertTrue(recList1.size() == 10);

        final String query2 = "SELECT * FROM table1";
        List<HRecord> recList2 = connection.executeQueryAndFetch(query2);
        assertTrue(recList2.size() == 10);

        for (final HRecord rec : recList2) {
            Map map1 = rec.getUnMappedValueMap("f3");
            assertTrue(map1.size() == 5);
        }
    }

    @Test
    public void selectUnnamedExpressions() throws HBqlException {

        connection.execute("DROP MAPPING tab8");

        connection.execute("CREATE TEMP MAPPING tab8 FOR TABLE table1"
                           + "("
                           + "keyval key, "
                           + "f1 INCLUDE UNMAPPED ("
                           + "  val1 string alias val1, "
                           + "  val10 string alias val10 "
                           + "))");

        final String query1 = "SELECT 2+4, 5+9, 5+3 as expr1, DEFINEDINROW(val1), DEFINEDINROW(val10) FROM tab8";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);
        assertTrue(recList1.size() == 10);
        for (final HRecord rec : recList1) {
            int val1 = (Integer)rec.getCurrentValue("expr-0");
            assertTrue(val1 == 6);
            int val2 = (Integer)rec.getCurrentValue("expr-1");
            assertTrue(val2 == 14);
            int val3 = (Integer)rec.getCurrentValue("expr1");
            assertTrue(val3 == 8);

            boolean val4 = (Boolean)rec.getCurrentValue("expr-2");
            assertTrue(val4);
            boolean val5 = (Boolean)rec.getCurrentValue("expr-3");
            assertTrue(!val5);
        }
    }

    @Test
    public void selectEvalExpressions() throws HBqlException {

        connection.execute("DROP MAPPING tab8");

        connection.execute("CREATE TEMP MAPPING tab8 FOR TABLE table1"
                           + "("
                           + "keyval key, "
                           + "f1 INCLUDE UNMAPPED ("
                           + "  val1 string alias val1, "
                           + "  val10 string alias val10 "
                           + "))");

        final String query1 = "SELECT EVAL('TRUE'), EVAL('FALSE') FROM tab8";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);
        assertTrue(recList1.size() == 10);
        for (final HRecord rec : recList1) {
            boolean val1 = (Boolean)rec.getCurrentValue("expr-0");
            assertTrue(val1);
            boolean val2 = (Boolean)rec.getCurrentValue("expr-1");
            assertTrue(!val2);
        }

        final String query2 = "SELECT EVAL(:val1), EVAL(:val2) FROM tab8";
        HPreparedStatement pstmt = connection.prepareStatement(query2);
        pstmt.setParameter("val1", "TRUE OR FALSE");
        pstmt.setParameter("val2", "TRUE AND FALSE");
        List<HRecord> recList2 = pstmt.executeQueryAndFetch();
        assertTrue(recList2.size() == 10);
        for (final HRecord rec : recList2) {
            boolean val1 = (Boolean)rec.getCurrentValue("expr-0");
            assertTrue(val1);
            boolean val2 = (Boolean)rec.getCurrentValue("expr-1");
            assertTrue(!val2);
        }
    }


    @Test
    public void selectDefaults() throws HBqlException {

        connection.execute("DROP MAPPING tab8");

        connection.execute("CREATE TEMP MAPPING tab8 FOR TABLE table1"
                           + "("
                           + "keyval key, "
                           + "f1 INCLUDE UNMAPPED ("
                           + "  val1 string alias val1, "
                           + "  val10 string alias val10 default 'test default', "
                           + "  val11 string alias val11  "
                           + "))");

        final String query1 = "SELECT * FROM tab8";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);
        assertTrue(recList1.size() == 10);
        for (final HRecord rec : recList1) {
            String val1 = (String)rec.getCurrentValue("val10");
            assertTrue(val1.equals("test default"));
            String val2 = (String)rec.getCurrentValue("val11");
            assertTrue(val2 == null);
        }

        final String query2 = "SELECT * FROM tab8 with client filter where val10 = 'test default'";
        List<HRecord> recList2 = connection.executeQueryAndFetch(query2);
        assertTrue(recList2.size() == 10);

        final String query3 = "SELECT * FROM tab8 with client filter where val11 = 'test default'";
        List<HRecord> recList3 = connection.executeQueryAndFetch(query3);
        assertTrue(recList3.size() == 0);
    }

    @Test
    public void selectMismatchedDefaults() throws HBqlException {

        connection.execute("DROP MAPPING tab8");

        Exception caughtException = null;
        try {
            connection.execute("CREATE TEMP MAPPING tab8 FOR TABLE table1"
                               + "(keyval key, f1 (val10 string alias val10 default 4))");
        }
        catch (Exception e) {
            e.printStackTrace();
            caughtException = e;
        }

        assertTrue(caughtException instanceof TypeException);
    }

    @Test
    public void selectObjectDefaults() throws HBqlException {

        connection.execute("DROP MAPPING tab8");
        Exception caughtException = null;
        try {
            connection.execute("CREATE TEMP MAPPING tab8 FOR TABLE table1"
                               + "(keyval key, f1 (val10 object alias val10 default 'test default'))");
        }
        catch (Exception e) {
            e.printStackTrace();
            caughtException = e;
        }

        assertTrue(caughtException == null);

        final String query1 = "SELECT * FROM tab8";
        List<HRecord> recList1 = connection.executeQueryAndFetch(query1);
        assertTrue(recList1.size() == 10);
        for (final HRecord rec : recList1) {
            String val1 = (String)rec.getCurrentValue("val10");
            assertTrue(val1.equals("test default"));
        }
    }
}