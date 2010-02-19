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
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;
import java.util.Random;

public class NullValuesTest extends TestSupport {

    static HConnection connection = null;

    static Random randomVal = new Random();

    @BeforeClass
    public static void beforeClass() throws HBqlException {

        connection = HConnectionManager.newConnection();

        connection.execute("CREATE TEMP MAPPING table30"
                           + "("
                           + "keyval key, "
                           + "f1 ("
                           + "  val1 string alias val1, "
                           + "  val2 date alias val2, "
                           + "  val3 int alias val3, "
                           + "  val4 int[] alias val4, "
                           + "  val5 object alias val5, "
                           + "  val6 string alias val6 "
                           + "))");

        if (!connection.tableExists("table30"))
            System.out.println(connection.execute("create table table30 (f1())"));
        else
            System.out.println(connection.execute("delete from table30"));

        HPreparedStatement stmt = connection.prepareStatement(
                "insert into table30 "
                + "(keyval, val1, val2, val3, val4, val5) values "
                + "(:key, :val1, :val2, :val3, :val4, :val5)");

        final String keyval = Util.getZeroPaddedNonNegativeNumber(1, TestSupport.keywidth);

        stmt.setParameter("key", keyval);
        stmt.setParameter("val1", null);
        stmt.setParameter("val2", new Date(System.currentTimeMillis()));
        stmt.setParameter("val3", 0);
        stmt.setParameter("val4", null);
        stmt.setParameter("val5", null);

        stmt.execute();
    }

    public void queryRecords(final int exepected, final String query) throws HBqlException {

        HResultSet<HRecord> resultSet = connection.executeQuery(query);

        System.out.println("Query: " + query);

        int rec_cnt = 0;
        for (HRecord rec : resultSet) {

            String key = (String)rec.getCurrentValue("keyval");
            String val1 = (String)rec.getCurrentValue("val1");
            Date val2 = (Date)rec.getCurrentValue("val2");
            int val3 = (Integer)rec.getCurrentValue("val3");
            int[] val4 = (int[])rec.getCurrentValue("val4");
            Object val5 = (Object)rec.getCurrentValue("val5");

            System.out.println("Current Values: " + key
                               + " - " + val1
                               + " - " + val2
                               + " - " + val3
                               + " - " + val4
                               + " - " + val5
                               + "\n");
            rec_cnt++;
        }

        assertTrue(rec_cnt == exepected);
    }


    @Test
    public void simpleInsert() throws HBqlException {

        queryRecords(1, "SELECT * FROM table30");
        queryRecords(1, "SELECT * FROM table30 with server filter where null is null");
        queryRecords(0, "SELECT * FROM table30 with server filter where null is not null");
        queryRecords(0, "SELECT * FROM table30 with server filter where val1 is not null");
        queryRecords(1, "SELECT * FROM table30 with server filter where val1 is null");
        queryRecords(0, "SELECT * FROM table30 with server filter where val2 is null");
        queryRecords(1, "SELECT * FROM table30 with server filter where val2 is not null");
        queryRecords(0, "SELECT * FROM table30 with server filter where val3 is null");
        queryRecords(1, "SELECT * FROM table30 with server filter where val3 is not null");
        queryRecords(0, "SELECT * FROM table30 with server filter where val4 is not null");
        queryRecords(1, "SELECT * FROM table30 with server filter where val4 is null");
        queryRecords(0, "SELECT * FROM table30 with server filter where val5 is not null");
        queryRecords(1, "SELECT * FROM table30 with server filter where val5 is null");
        queryRecords(0, "SELECT * FROM table30 with server filter where definedInRow(val6)");
        queryRecords(1, "SELECT * FROM table30 with server filter where not definedInRow(val6)");
    }
}