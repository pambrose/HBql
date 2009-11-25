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

package org.apache.expreval.examples;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.Util;

import java.util.Date;
import java.util.Map;

public class RecordExample {

    public static void main(String[] args) throws HBqlException {

        HConnection connection = HConnectionManager.newConnection();

        connection.execute("CREATE TEMP MAPPING testobjects alias testobjects2"
                           + "("
                           + "keyval key, "
                           + "family1:author string alias author, "
                           + "family1:title string  alias title, "
                           + "family1:intValue int alias comp1"
                           + "f3:mapval1 string alias f3mapval1, "
                           + "f3:mapval2 string alias f3mapval2 "
                           + ")");

        // System.out.println(conn.execute("delete from TestObject with client filter where true"));
        // System.out.println(conn.execute("disable table testobjects"));
        // System.out.println(conn.execute("enable table testobjects"));
        // System.out.println(conn.execute("drop table testobjects"));

        System.out.println(connection.execute("LIST TABLES"));

        final HBatch<HRecord> batch = HBatch.newHBatch(connection);
        for (int i = 0; i < 10; i++) {
            HRecord record = connection.getMapping("testobjects").newHRecord();
            record.setCurrentValue("keyval", Util.getZeroPaddedNumber(i, 10));
            record.setCurrentValue("author", "A new author value: " + i);
            record.setCurrentValue("title", "A very new title value: " + i);
            batch.insert(record);
        }

        batch.apply();

        if (connection.tableEnabled("testobjects2"))
            System.out.println(connection.execute("describe table testobjects2"));

        final String query1 = "SELECT keyval, author, title, (3*12) as comp1 "
                              + "FROM testobjects2 "
                              + "WITH "
                              + "KEYS '0000000002' TO '0000000003', '0000000008' TO LAST "
                              + "TIME RANGE NOW()-DAY(25) TO NOW()+DAY(1)"
                              + "VERSIONS 2 "
                              //+ "SCAN LIMIT 4"
                              //+ "SERVER FILTER WHERE author LIKE '.*6200.*' "
                              + "CLIENT FILTER WHERE keyval = '0000000002' OR author LIKE '.*val.*'";
        HResultSet<HRecord> results1 = connection.executeQuery(query1);

        for (HRecord val1 : results1) {
            System.out.println("Current Values: " + val1.getCurrentValue("keyval")
                               + " - " + val1.getCurrentValue("family1:author")
                               + " - " + val1.getCurrentValue("title")
                               + " - " + val1.getCurrentValue("comp1")
            );

            System.out.println("Versions");

            if (val1.getVersionMap("author") != null) {
                Map<Long, Object> versioned = val1.getVersionMap("family1:author");
                for (final Long key : versioned.keySet())
                    System.out.println(new Date(key) + " - " + versioned.get(key));
            }

            if (val1.getVersionMap("family1:title") != null) {
                Map<Long, Object> versioned = val1.getVersionMap("title");
                for (final Long key : versioned.keySet())
                    System.out.println(new Date(key) + " - " + versioned.get(key));
            }
        }
        results1.close();
    }
}