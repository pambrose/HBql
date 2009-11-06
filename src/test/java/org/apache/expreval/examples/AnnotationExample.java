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

import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.client.Batch;
import org.apache.hadoop.hbase.hbql.client.Column;
import org.apache.hadoop.hbase.hbql.client.ColumnVersionMap;
import org.apache.hadoop.hbase.hbql.client.Connection;
import org.apache.hadoop.hbase.hbql.client.ConnectionManager;
import org.apache.hadoop.hbase.hbql.client.Family;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.Query;
import org.apache.hadoop.hbase.hbql.client.Results;
import org.apache.hadoop.hbase.hbql.client.Table;
import org.apache.hadoop.hbase.hbql.client.Util;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class AnnotationExample {

    @Table(name = "testobjects",
           families = {
                   @Family(name = "family1", maxVersions = 10),
                   @Family(name = "family2"),
                   @Family(name = "family3", maxVersions = 5)
           })
    public static class TestObject {

        private enum TestEnum {
            RED, BLUE, BLACK, ORANGE
        }

        @Column(key = true)
        public String keyval;

        @Column(family = "family1")
        public TestEnum enumValue = TestEnum.BLUE;

        @Column(family = "family1")
        public int intValue = -1;

        @Column(family = "family1")
        public String strValue = "";

        @Column(family = "family1")
        public String title = "";

        @ColumnVersionMap(instance = "title")
        public NavigableMap<Long, String> titles = new TreeMap<Long, String>();

        @Column(family = "family1", column = "author")
        public String author = "";

        @ColumnVersionMap(instance = "author")
        public NavigableMap<Long, String> authorVersions;

        @Column(family = "family3", familyDefault = true)
        public Map<String, byte[]> family1Default = Maps.newHashMap();

        @ColumnVersionMap(instance = "family1Default")
        public Map<String, NavigableMap<Long, byte[]>> family1DefaultVersions;

        @Column(family = "family2", getter = "getHeaderBytes", setter = "setHeaderBytes")
        public String header = "A header value";

        @Column(family = "family2", column = "bodyimage")
        public String bodyimage = "A bodyimage value";

        @Column(family = "family2")
        public int[] array1 = {1, 2, 3};

        @Column(family = "family2")
        public String[] array2 = {"val1", "val2", "val3"};

        @Column(family = "family3")
        public Map<String, String> mapval1 = Maps.newHashMap();

        @ColumnVersionMap(instance = "mapval1")
        public Map<String, NavigableMap<Long, String>> mapval1Versions;

        @Column(family = "family3")
        public Map<String, String> mapval2 = Maps.newHashMap();

        public TestObject() {
        }

        public TestObject(int val) throws HBqlException {
            this.keyval = Util.getZeroPaddedNumber(val, 6);

            this.title = "A title value";
            this.author = "An author value";
            strValue = "v" + val;

            mapval1.put("key1", "val1");
            mapval1.put("key2", "val2");

            mapval2.put("key3", "val3");
            mapval2.put("key4", "val4");

            author += "-" + val + System.nanoTime();
            header += "-" + val;
            title += "-" + val;
        }

        public byte[] getHeaderBytes() {
            return this.header.getBytes();
        }

        public void setHeaderBytes(byte[] val) {
            this.header = new String(val);
        }
    }

    public static void main(String[] args) throws IOException, HBqlException {

        Connection conn = ConnectionManager.newConnection();

        /*
        if (conn.tableExists("TestObject")) {
            System.out.println(conn.execute("disable table TestObject"));
            System.out.println(conn.execute("drop table TestObject"));
        }
        */

        if (!conn.tableExists("TestObject")) {
            System.out.println(conn.execute("create table with schema TestObject"));

            final Batch batch = new Batch();
            for (int i = 0; i < 10; i++)
                batch.insert(new TestObject(i));

            conn.apply(batch);
        }

        final String query2 = "SELECT title, titles, author, authorVersions "
                              + "FROM TestObject "
                              + "WITH "
                              + "KEYS '0000000002' TO '0000000003', '0000000007' TO '0000000008' "
                              + "TIME RANGE NOW()-DAY(25) TO NOW()+DAY(1) "
                              + "VERSIONS 3 "
                              //+ "SERVER FILTER WHERE author LIKE '.*val.*' OR LENGTH(author) > 4 "
                              + "CLIENT FILTER WHERE author LIKE '.*val.*' OR LENGTH(author) > 4";

        Query<TestObject> q2 = conn.newQuery(query2);
        Results<TestObject> results2 = q2.getResults();

        for (TestObject val2 : results2) {
            System.out.println("Current Values: " + val2.keyval + " - " + val2.author + " - " + val2.title);

            System.out.println("Historicals");
            if (val2.authorVersions != null)
                for (final Long key : val2.authorVersions.keySet())
                    System.out.println(new Date(key) + " - " + val2.authorVersions.get(key));

            if (val2.titles != null)
                for (final Long key : val2.titles.keySet())
                    System.out.println(new Date(key) + " - " + val2.titles.get(key));
        }

        results2.close();
    }
}
