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
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Random;

public class DeleteTest extends TestSupport {

    static HConnection connection = null;

    static Random randomVal = new Random();

    static int count = 10;

    @BeforeClass
    public static void beforeClass() throws HBqlException {

        connection = HConnectionManager.newConnection();

        if (connection.tableExists("delete_test")) {
            System.out.println(connection.execute("disable table delete_test"));
            System.out.println(connection.execute("drop table delete_test"));
        }

        System.out.println(connection.execute("create table delete_test (f1, f2, f3)"));

        connection.execute("CREATE TEMP MAPPING delete_test "
                           + "("
                           + "keyval key, "
                           + "f1 ("
                           + "  val1 string alias val1, "
                           + "  val2 string alias val2, "
                           + "  val3 string alias val3 "
                           + "), "
                           + "f2 ("
                           + "  val1 string alias val11, "
                           + "  val2 string alias val12, "
                           + "  val3 string alias val13 "
                           + "), "
                           + "f3 ("
                           + "  val1 string alias val21, "
                           + "  val2 string alias val22, "
                           + "  val3 string alias val23 "
                           + "))");

        insertRecords(connection, count, "Batch 1");
        insertRecords(connection, count, "Batch 2");

        insertRecords(connection, count, "Batch 3");
    }

    private static void insertRecords(final HConnection connection,
                                      final int cnt,
                                      final String msg) throws HBqlException {

        HPreparedStatement stmt = connection.prepareStatement(
                "insert into delete_test " +
                "(keyval, val1, val2, val3, val11, val12, val13, val21, val22, val23 ) values " +
                "(:key, :val1, :val2, :val3, :val11, :val12, :val13, :val21, :val22, :val23)");

        for (int i = 0; i < cnt; i++) {

            final String keyval = Util.getZeroPaddedNumber(i, TestSupport.keywidth);

            int randomNum = randomVal.nextInt();
            String randomStr = "" + randomNum;

            stmt.setParameter("key", keyval);
            stmt.setParameter("val1", randomStr);
            stmt.setParameter("val2", randomStr + " " + msg);
            stmt.setParameter("val3", randomStr + " " + msg + " " + msg);
            stmt.setParameter("val11", randomStr);
            stmt.setParameter("val12", randomStr + " " + msg);
            stmt.setParameter("val13", randomStr + " " + msg + " " + msg);
            stmt.setParameter("val21", randomStr);
            stmt.setParameter("val22", randomStr + " " + msg);
            stmt.setParameter("val23", randomStr + " " + msg + " " + msg);
            stmt.execute();
        }
    }

    @Test
    public void deleteColumn() throws HBqlException {

        String sql = "SELECT count() as cnt FROM delete_test WITH CLIENT FILTER WHERE definedinrow(f1:val1)";
        List<HRecord> recs = connection.executeQueryAndFetch(sql);
        HRecord rec = recs.get(0);
        long cnt = (Long)rec.getCurrentValue("cnt");
        assertTrue(cnt == count);

        connection.executeUpdate("DELETE f1:val1 FROM delete_test");

        recs = connection.executeQueryAndFetch(sql);
        rec = recs.get(0);
        cnt = (Long)rec.getCurrentValue("cnt");
        assertTrue(cnt == 0);
    }

    @Test
    public void deleteColumns() throws HBqlException {

        String sql = "SELECT count() as cnt FROM delete_test " +
                     "WITH CLIENT FILTER WHERE definedinrow(val11) AND definedinrow(val12)";
        List<HRecord> recs = connection.executeQueryAndFetch(sql);
        HRecord rec = recs.get(0);
        long cnt = (Long)rec.getCurrentValue("cnt");
        assertTrue(cnt == count);

        connection.executeUpdate("DELETE val11, f2:val2 FROM delete_test");

        recs = connection.executeQueryAndFetch(sql);
        rec = recs.get(0);
        cnt = (Long)rec.getCurrentValue("cnt");
        assertTrue(cnt == 0);
    }
}