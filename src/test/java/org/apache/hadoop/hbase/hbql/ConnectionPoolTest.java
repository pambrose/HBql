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
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HConnectionPool;
import org.apache.hadoop.hbase.hbql.client.HConnectionPoolManager;
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ConnectionPoolTest extends TestSupport {

    static HConnection connection = null;

    static HConnectionPool connectionPool = null;
    static ExecutorService exec = null;

    static Random randomVal = new Random();

    static int count = 10;
    static int workerCount = 10;
    final static int clients = 100;
    final static int iterations = 100;

    List<Exception> exceptionList = Lists.newArrayList();

    @BeforeClass
    public static void beforeClass() throws HBqlException {

        exec = Executors.newFixedThreadPool(workerCount);

        connectionPool = HConnectionPoolManager.newConnectionPool(5);
        connection = HConnectionManager.newConnection();

        /*
        System.out.println(connection.execute("disable table pool_test if tableExists('pool_test')"));
        System.out.println(connection.execute("drop table pool_test if tableExists('pool_test')"));
        System.out.println(connection.execute("create table pool_test (f1(), f2(), f3())"));
         */
        connection.execute("CREATE TEMP MAPPING pool_test "
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
                "insert into pool_test " +
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

    void doQuery(final String id, final int iteration) throws HBqlException {

        HConnection connection = connectionPool.getConnection();
        connection.execute("CREATE TEMP MAPPING pool_test "
                           + "("
                           + "keyval key, "
                           + "f1 ("
                           + "  val1 string alias val1, "
                           + "  val2 string alias val2, "
                           + "  val3 string alias val3 "
                           + ") "
                           + ") if not mappingexists('pool_test')");

        String sql = "SELECT count() as cnt FROM pool_test WITH CLIENT FILTER WHERE definedinrow(f1:val1)";
        List<HRecord> recs = connection.executeQueryAndFetch(sql);
        HRecord rec = recs.get(0);
        long cnt = (Long)rec.getCurrentValue("cnt");
        assertTrue(cnt == count);

        connection.close();

        System.out.println("Completed: " + id + " - " + iteration);
        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            exceptionList.add(e);
        }
    }

    @Test
    public void simpleTest() throws HBqlException, ExecutionException, InterruptedException {

        List<Future> futureList = Lists.newArrayList();
        for (int i = 0; i < clients; i++) {
            final String id = "" + i;
            System.out.println("Created: " + i);
            Future future = exec.submit(new Runnable() {
                public void run() {
                    try {
                        for (int i = 0; i < iterations; i++)
                            doQuery(id, i);

                        try {
                            Thread.sleep(10);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    catch (HBqlException e) {
                        e.printStackTrace();
                    }
                }
            });
            futureList.add(future);
        }

        for (final Future future : futureList) {
            future.get();
        }

        assertTrue(exceptionList.size() == 0);
    }
}