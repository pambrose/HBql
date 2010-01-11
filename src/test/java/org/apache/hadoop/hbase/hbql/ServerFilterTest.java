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

import org.apache.hadoop.hbase.hbql.client.AsyncExecutorManager;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HConnectionPool;
import org.apache.hadoop.hbase.hbql.client.HConnectionPoolManager;
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.HStatement;
import org.apache.hadoop.hbase.hbql.client.QueryExecutorPoolManager;
import org.apache.hadoop.hbase.hbql.client.QueryFuture;
import org.apache.hadoop.hbase.hbql.client.QueryListener;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.util.Lists;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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

    private static void showValues(final String sql, final int cnt, final boolean printValues) throws HBqlException {

        HStatement stmt = connection.createStatement();
        HResultSet<HRecord> results = stmt.executeQuery(sql);

        int rec_cnt = 0;
        for (HRecord rec : results) {

            String keyval = (String)rec.getCurrentValue("keyval");
            String val1 = (String)rec.getCurrentValue("val1");
            int val2 = (Integer)rec.getCurrentValue("f1:val2");
            int val3 = (Integer)rec.getCurrentValue("val3");
            if (printValues)
                System.out.println("Current Values: " + keyval + " : " + val1 + " : " + val2 + " : " + val3);
            rec_cnt++;
        }

        results.close();

        assertTrue(rec_cnt == cnt);
    }

    private static int showValues(final HConnection conn,
                                  final String sql,
                                  final int cnt,
                                  final boolean printValues) throws HBqlException {

        HStatement stmt = conn.createStatement();
        HResultSet<HRecord> results = stmt.executeQuery(sql);

        int rec_cnt = 0;
        for (HRecord rec : results) {

            String keyval = (String)rec.getCurrentValue("keyval");
            String val1 = (String)rec.getCurrentValue("val1");
            int val2 = (Integer)rec.getCurrentValue("f1:val2");
            int val3 = (Integer)rec.getCurrentValue("val3");
            if (printValues)
                System.out.println("Current Values: " + keyval + " : " + val1 + " : " + val2 + " : " + val3);
            rec_cnt++;
        }

        results.close();

        assertTrue(rec_cnt == cnt);

        return rec_cnt;
    }

    @Test
    public void simpleSelect1() throws HBqlException {
        final String q1 = "select * from tab3";
        showValues(q1, 10, true);
    }

    @Test
    public void simpleSelect2() throws HBqlException {
        final String q1 = "select * from tab3 WITH SERVER FILTER " +
                          "where val1 = '11' OR val1 = '14' OR  val1 = '12'";
        showValues(q1, 3, true);
    }

    @Test
    public void simpleSelect3() throws HBqlException {
        final String q1 = "select * from tab3 WITH SERVER FILTER where val1 <= '12' ";
        showValues(q1, 3, true);
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
        showValues(q1, 1, true);
    }

    @Test
    public void simpleSelect5() throws HBqlException {
        final String q1 = "select * from tab3 WITH SERVER FILTER where val2 BETWEEN 12 AND 14 ";
        showValues(q1, 3, true);
    }

    @Test
    public void simpleSelect6() throws HBqlException {
        for (int i = 0; i < 100; i++) {
            final String q1 = "select * from tab3 WITH SERVER FILTER where val1 BETWEEN '12' AND '14' ";
            System.out.println("Vals: " + i);
            showValues(q1, 3, false);
        }
    }

    @Test
    public void simpleSelect6b() throws HBqlException {
        for (int i = 0; i < 100; i++) {
            final String q1 = "select * from tab3 WITH SERVER FILTER where val1 IN ('12', '13', '14') ";
            System.out.println("Vals: " + i);
            showValues(q1, 3, false);
        }
    }

    @Test
    public void simpleSelect7() throws HBqlException {
        final String q1 = "select * from tab3 WITH SERVER FILTER where val1+'ss' BETWEEN '12ss' AND '14ss' ";
        showValues(q1, 3, true);
    }

    @Test
    public void simpleSelect8() throws HBqlException {
        final String q1 = "select * from tab3 WITH "
                          + "KEYS '0000000001', '0000000002', '0000000003' ";
        //+ "SERVER FILTER where val1+'ss' BETWEEN '12ss' AND '14ss' ";
        showValues(q1, 3, true);
    }

    @Test
    public void simpleSelect9a() throws HBqlException {

        HStatement stmt = connection.createStatement();
        System.out.println(stmt.execute("CREATE QUERY EXECUTOR POOL threadPool1 " +
                                        "(max_executor_pool_size: 5, max_thread_count: 2, " +
                                        "threads_read_results: true, completion_queue_size: 100) " +
                                        "if not queryExecutorPoolExists('threadPool1')"));

        // ExecutorPoolManager.newExecutorPool("threadPool1", 2, 10);
        connection.setQueryExecutorPoolName("threadPool1");

        for (int i = 0; i < 100; i++) {
            final String q1 = "select * from tab3 "
                              + "WITH "
                              + "KEYS '0000000001', '0000000002', '0000000003', '0000000003', '0000000004', '0000000005'  "
                              //+ "KEYS '0000000001'TO '0000000005' "
                              //+ "KEYS  '0000000005' "
                              //+ "SERVER FILTER where keyval BETWEEN '0000000001' AND '0000000003' ";
                              // + "SERVER FILTER where keyval = '0000000001' ";
                              + "SERVER FILTER where val1+'ss' BETWEEN '11ss' AND '13ss' ";

            showValues(q1, 4, false);
        }
    }

    @Test
    public void simpleSelect10() throws HBqlException {

        HStatement stmt = connection.createStatement();

        System.out.println(stmt.execute("CREATE QUERY EXECUTOR POOL threadPool1 " +
                                        "(max_executor_pool_size: 5, max_thread_count: 4, " +
                                        "threads_read_results: true, completion_queue_size: 100) " +
                                        "if not queryExecutorPoolExists('threadPool1')"));
        System.out
                .println(stmt.execute("DROP QUERY EXECUTOR POOL threadPool1 if queryExecutorPoolExists('threadPool1')"));
        System.out.println(stmt.execute("CREATE QUERY EXECUTOR POOL threadPool1 " +
                                        "(max_executor_pool_size: 5, max_thread_count: 4, " +
                                        "threads_read_results: true, completion_queue_size: 100)"));

        connection.setQueryExecutorPoolName("threadPool1");

        for (int i = 0; i < 50; i++) {
            final StringBuilder q1 = new StringBuilder("select * from tab3 WITH "
                                                       + "KEYS ");
            boolean firstTime = true;
            int cnt = 30;
            for (int j = 0; j < cnt; j++) {
                if (!firstTime)
                    q1.append(", ");
                else
                    firstTime = false;
                q1.append("'0000000001'TO '0000000009'");
            }
            q1.append("SERVER FILTER where val1+'ss' BETWEEN '11ss' AND '13ss' ");
            System.out.println("Values for iteration:" + i);
            showValues(q1.toString(), cnt * 3, false);
        }
    }

    @Test
    public void asyncSelect1() throws HBqlException {

        QueryExecutorPoolManager.newQueryExecutorPool("threadPool2", 5, 2, 5, 30, true, 100);
        AsyncExecutorManager.newAsyncExecutor("asyncPool2", 2, 4, 60);
        //connection.execute("create async executor asyncPool2");
        connection.setQueryExecutorPoolName("threadPool2");
        connection.setAsyncExecutorName("asyncPool2");

        final List<QueryFuture> futureList = Lists.newArrayList();

        int repeats = 25;
        for (int i = 0; i < repeats; i++) {
            int queryCount = Utils.getRandomPositiveInt(100);
            for (int j = 0; j < queryCount; j++) {
                final StringBuilder q1 = new StringBuilder("select * from tab3 WITH KEYS ");
                boolean firstTime = true;
                final int keyCount = Utils.getRandomPositiveInt(100);
                for (int k = 0; k < keyCount; k++) {
                    if (!firstTime)
                        q1.append(", ");
                    else
                        firstTime = false;
                    q1.append("'0000000001'TO '0000000009'");
                }
                q1.append("SERVER FILTER where val1+'ss' BETWEEN '11ss' AND '13ss' ");

                HStatement stmt = connection.createStatement();
                QueryFuture future = stmt.executeQueryAsync(q1.toString(), new QueryListener<HRecord>() {
                    AtomicInteger rec_cnt = new AtomicInteger(0);

                    public void onQueryStart() {
                        //System.out.println("Starting query");
                    }

                    public void onEachRow(final HRecord rec) throws HBqlException {
                        String keyval = (String)rec.getCurrentValue("keyval");
                        String val1 = (String)rec.getCurrentValue("val1");
                        int val2 = (Integer)rec.getCurrentValue("f1:val2");
                        int val3 = (Integer)rec.getCurrentValue("val3");
                        //System.out.println("Current Values: " + keyval + " : " + val1 + " : " + val2 + " : " + val3);
                        this.rec_cnt.incrementAndGet();
                    }

                    public void onQueryComplete() {
                        System.out.println("Finished query with rec_cnt = " + rec_cnt.get() + " vs " + (keyCount * 3));
                        assertTrue(rec_cnt.get() == keyCount * 3);
                    }

                    public void onException(final ExceptionSource source, final HBqlException e) {
                        e.printStackTrace();
                    }
                });
                futureList.add(future);
            }

            for (final QueryFuture future : futureList) {
                try {
                    future.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void randomConcurrentTest() throws HBqlException, InterruptedException {

        final HConnectionPool connectionPool = HConnectionPoolManager.newConnectionPool(1, 4);

        final int queryPoolCnt = 5;
        for (int p = 1; p <= queryPoolCnt; p++) {
            final String poolName = "execPool" + p;
            System.out.println("Creating query executor pool: " + poolName);
            QueryExecutorPoolManager.newQueryExecutorPool(poolName,
                                                          Utils.getRandomPositiveInt(5),
                                                          Utils.getRandomPositiveInt(2),
                                                          Utils.getRandomPositiveInt(5),
                                                          Long.MAX_VALUE,
                                                          Utils.getRandomBoolean(),
                                                          Utils.getRandomPositiveInt(10));
        }

        final int poolSize = Utils.getRandomPositiveInt(10);
        final ExecutorService threadPool = Executors.newFixedThreadPool(poolSize);

        final int repeats = 10;

        for (int i = 0; i < repeats; i++) {
            final int totalJobs = Utils.getRandomPositiveInt(50);
            final int keyCount = Utils.getRandomPositiveInt(50);
            final CountDownLatch latch = new CountDownLatch(totalJobs);

            for (int tj = 0; tj < totalJobs; tj++) {
                final int jobNum = tj;
                threadPool.submit(
                        new Runnable() {
                            public void run() {

                                HConnection conn = null;
                                try {
                                    conn = connectionPool.takeConnection();

                                    conn.setQueryExecutorPoolName("execPool" + Utils.getRandomPositiveInt(queryPoolCnt));

                                    conn.execute("CREATE TEMP MAPPING tab3 FOR TABLE table20"
                                                 + "("
                                                 + "keyval key, "
                                                 + "f1 ("
                                                 + "  val1 string alias val1, "
                                                 + "  val2 int alias val2, "
                                                 + "  val3 int alias val3 DEFAULT 12 "
                                                 + "))");

                                    final StringBuilder query = new StringBuilder("select * from tab3 WITH KEYS ");
                                    final int rangeCount = Utils.getRandomPositiveInt(keyCount);
                                    boolean firstTime = true;
                                    for (int rc = 0; rc < rangeCount; rc++) {
                                        if (!firstTime)
                                            query.append(", ");
                                        else
                                            firstTime = false;
                                        query.append("'0000000001'TO '0000000009' ");
                                    }

                                    query.append("SERVER FILTER where val1+'ss' BETWEEN '11ss' AND '13ss' ");

                                    int recCnt = showValues(conn, query.toString(), rangeCount * 3, false);
                                    System.out.println("Value count: " + recCnt + " for job: " + jobNum);
                                }
                                catch (HBqlException e) {
                                    e.printStackTrace();
                                }
                                finally {
                                    if (conn != null) {
                                        conn.close();
                                    }
                                    latch.countDown();
                                }
                            }
                        });
            }

            latch.await();
        }
    }
}