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

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HMapping;
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

public class ExamplesTest extends TestSupport {

    public void showTables() throws HBqlException {

        // START SNIPPET: show-tables

        HConnection connection = HConnectionManager.newConnection();
        System.out.println(connection.execute("SHOW TABLES"));

        // Or using the API
        Set<String> tableNamess = connection.getTableNames();

        // END SNIPPET: show-tables
    }

    public void showSchemas() throws HBqlException {

        // START SNIPPET: show-schemas

        HConnection connection = HConnectionManager.newConnection();

        System.out.println(connection.execute("SHOW SCHEMAS"));

        // Or using the API
        Set<HMapping> mappings = connection.getSchemas();
        for (HMapping mapping : mappings)
            System.out.println(mapping.getMappingName());

        // END SNIPPET: show-schemas
    }

    public void describeTable() throws HBqlException {

        // START SNIPPET: describe-table

        HConnection connection = HConnectionManager.newConnection();
        System.out.println(connection.execute("DESCRIBE TABLE foo"));

        // END SNIPPET: describe-table

    }

    public void describeSchema() throws HBqlException {

        // START SNIPPET: describe-schema

        HConnection connection = HConnectionManager.newConnection();
        System.out.println(connection.execute("DESCRIBE SCHEMA foo_schema"));

        // END SNIPPET: describe-schema

    }

    public void enableTable() throws HBqlException {

        // START SNIPPET: enable-table

        HConnection connection = HConnectionManager.newConnection();
        System.out.println(connection.execute("ENABLE TABLE foo"));

        // Or using the API
        connection.enableTable("foo");

        // END SNIPPET: enable-table

    }

    public void disableTable() throws HBqlException {

        // START SNIPPET: disable-table

        HConnection connection = HConnectionManager.newConnection();
        connection.execute("DISABLE TABLE foo");

        // Or using the API
        connection.disableTable("foo");

        // END SNIPPET: disable-table

    }

    public void dropSchema() throws HBqlException {

        // START SNIPPET: drop-schema

        HConnection connection = HConnectionManager.newConnection();

        connection.execute("DROP SCHEMA foo_schema");

        // Or using the API
        connection.dropSchema("foo_schema");

        // END SNIPPET: drop-schema

    }

    public void createTable() throws HBqlException {

        // START SNIPPET: create-table

        HConnection connection = HConnectionManager.newConnection();
        connection.execute("CREATE TABLE foo (family1 (MAX VERSIONS 10), family2, family3)");

        // END SNIPPET: create-table

    }

    public void dropTable() throws HBqlException {

        // START SNIPPET: drop-table

        HConnection conection = HConnectionManager.newConnection();
        conection.execute("DROP TABLE foo");

        // Or using the API
        conection.dropTable("foo");

        // END SNIPPET: drop-table

    }

    public void insert1() throws HBqlException {

        // START SNIPPET: insert1

        HConnection connection = HConnectionManager.newConnection();

        connection.execute("CREATE TEMP SCHEMA foo_schema FOR TABLE foo "
                           + "("
                           + "keyval KEY, "
                           + "family1 ("
                           + "  val1 INT ALIAS val1, "
                           + "  val2 STRING ALIAS val2"
                           + "))");

        System.out.println(connection.execute("INSERT INTO foo_schema (keyval, val1, val2) "
                                              + "VALUES (ZEROPAD(2, 10), 123, 'test val')"));

        // Or using the Record interface
        HRecord rec = connection.getMapping("foo_schema").newHRecord();
        rec.setCurrentValue("keyval", Util.getZeroPaddedNumber(2, 10));
        rec.setCurrentValue("val1", 123);
        rec.setCurrentValue("al2", "testval");

        HBatch batch = new HBatch(connection);
        batch.insert(rec);
        batch.apply();

        // END SNIPPET: insert1

    }

    public void insert2() throws HBqlException {

        // START SNIPPET: insert2

        HConnection connection = HConnectionManager.newConnection();

        // A column with a default value.
        connection.execute("CREATE TEMP SCHEMA foo_schema FOR TABLE foo "
                           + "("
                           + "keyval KEY, "
                           + "family1 ("
                           + "  val1 INT ALIAS val1, "
                           + "  val2 STRING ALIAS val2 DEFAULT 'this is a default value'"
                           + "))");

        HPreparedStatement ps = connection.prepareStatement("INSERT INTO foo_schema (keyval, val1, val2) "
                                                            + "VALUES (:key, :val1, DEFAULT)");

        ps.setParameter("key", Util.getZeroPaddedNumber(2, 10));
        ps.setParameter("val1", 123);

        System.out.println(ps.execute());
        // END SNIPPET: insert2

    }

    public void insert3() throws HBqlException {

        HConnection connection = HConnectionManager.newConnection();

        // START SNIPPET: insert3
        connection.execute("CREATE SCHEMA foo_schema FOR TABLE foo "
                           + "("
                           + "keyval KEY, "
                           + "family1 ("
                           + "  val1 STRING ALIAS val1, "
                           + "  val2 STRING ALIAS val2, "
                           + "  val3 STRING ALIAS val3, "
                           + "  val4 STRING ALIAS val4 "
                           + "))");
        System.out.println(connection.execute("INSERT INTO foo_schema (keyval, val1, val2) "
                                              + "SELECT keyval, val3, val4 FROM foo2_schema"));
        // END SNIPPET: insert3

    }


    public void createSchema() throws HBqlException {

        // START SNIPPET: create-schema1

        HConnection connection = HConnectionManager.newConnection();

        // Schema named foo that corresponds to table foo.
        connection.execute("CREATE TEMP SCHEMA foo (keyval key, family1 (val1 STRING))");
        // END SNIPPET: create-schema1

        // START SNIPPET: create-schema2
        // Schema named schema1 that corresponds to table foo.
        connection.execute("CREATE SCHEMA schema1 FOR TABLE foo (keyval key, family1 (val1 STRING ALIAS val2))");
        // END SNIPPET: create-schema2

        // START SNIPPET: create-schema3
        // A column with a default value.
        connection.execute("CREATE SCHEMA schema1 FOR TABLE foo "
                           + "("
                           + "keyval key, "
                           + "family1 (val1 STRING ALIAS val1 DEFAULT 'this is a default value')"
                           + ")");
        // END SNIPPET: create-schema3

        // START SNIPPET: create-schema4

        // A schema with a family default attribute.
        connection.execute("CREATE TEMP SCHEMA schema1 FOR TABLE foo "
                           + "("
                           + "keyval key, "
                           + "family1 INCLUDE FAMILY DEFAULT ("
                           + "  val1 STRING ALIAS val1, "
                           + "  val2 STRING ALIAS val3 "
                           + "))");

        // END SNIPPET: create-schema4

    }

    @Test
    public void selectAll() throws HBqlException {

        // START SNIPPET: select1

        HConnection connection = HConnectionManager.newConnection();

        connection.execute("CREATE TEMP SCHEMA tab1 FOR TABLE table1"
                           + "("
                           + "keyval KEY, "
                           + "f1 INCLUDE FAMILY DEFAULT ("
                           + "  val1 STRING ALIAS val1, "
                           + "  val1 INT ALIAS val5"
                           + "),  "
                           + "f2 INCLUDE FAMILY DEFAULT, "
                           + "f3 INCLUDE FAMILY DEFAULT ("
                           + "  val2 INT ALIAS val6, "
                           + "  val3 INT ALIAS val7 "
                           + "))");

        HPreparedStatement pstmt = connection.prepareStatement("SELECT keyval, f1:val1, val5 FROM tab1 "
                                                               + "WITH KEYS FIRST TO :endkey "
                                                               + "VERSIONS 4 "
                                                               + "CLIENT FILTER WHERE val6 > 4");

        pstmt.setParameter("endkey", Util.getZeroPaddedNumber(34, 10));

        HResultSet<HRecord> records = pstmt.executeQuery();

        for (HRecord record : records) {
            System.out.println("Key = " + record.getCurrentValue("keyval"));
        }

        // END SNIPPET: select1
    }

    @Test
    public void definedSelect() throws HBqlException {

        // START SNIPPET: definedExample1

        // Get a connection to HBase
        HConnection connection = HConnectionManager.newConnection();

        // CREATE TEMP SCHEMA
        connection.execute("CREATE TEMP SCHEMA demo1 FOR TABLE example1"
                           + "("
                           + "keyval KEY, "
                           + "f1 ("
                           + "  val1 STRING ALIAS val1, "
                           + "  val2 INT ALIAS val2, "
                           + "  val3 STRING DEFAULT 'This is a default value' "
                           + "))");

        // Clean up table
        if (!connection.tableExists("example1"))
            System.out.println(connection.execute("CREATE TABLE example1 (f1) "));
        else
            System.out.println(connection.execute("DELETE FROM demo1"));

        // Add some records using an INSERT stmt
        HPreparedStatement stmt = connection.prepareStatement("INSERT INTO demo1 " +
                                                              "(keyval, val1, val2, f1:val3) VALUES " +
                                                              "(ZEROPAD(:key, 10), :val1, :val2, DEFAULT)");

        for (int i = 0; i < 5; i++) {
            stmt.setParameter("key", i);
            stmt.setParameter("val1", "Value: " + i);
            stmt.setParameter("val2", i);
            stmt.execute();
        }

        // Add some other records using the Record interface
        final HBatch batch = new HBatch(connection);
        for (int i = 5; i < 10; i++) {
            HRecord rec = connection.getMapping("demo1").newHRecord();
            rec.setCurrentValue("keyval", Util.getZeroPaddedNumber(i, 10));
            rec.setCurrentValue("val1", "Value: " + i);
            rec.setCurrentValue("f1:val2", i);
            batch.insert(rec);
        }
        batch.apply();

        // Query the records just added
        HResultSet<HRecord> records = connection.executeQuery("SELECT * FROM demo1");

        for (HRecord rec : records) {
            System.out.println("Key = " + rec.getCurrentValue("keyval"));
            System.out.println("f1:val1 = " + rec.getCurrentValue("val1"));
            System.out.println("f1:val2 = " + rec.getCurrentValue("f1:val2"));
            System.out.println("f1:val3 = " + rec.getCurrentValue("f1:val3"));
        }

        // END SNIPPET: definedExample1
    }

    @Test
    public void jdbc1() throws SQLException, ClassNotFoundException {

        // START SNIPPET: jdbc1

        Class.forName("org.apache.hadoop.hbase.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:hbql");

        Statement stmt = connection.createStatement();
        stmt.execute("CREATE TABLE table12 (f1, f3)");

        stmt.execute("CREATE TEMP SCHEMA sch9 FOR TABLE table12"
                     + "("
                     + "keyval key, "
                     + "f1 ("
                     + "    val1 string alias val1, "
                     + "    val2 string alias val2, "
                     + "), "
                     + "f3 ("
                     + "    val1 int alias val5, "
                     + "    val2 int alias val6 "
                     + "))");

        ResultSet rs = stmt.executeQuery("select * from sch9");

        while (rs.next()) {
            int val5 = rs.getInt("val5");
            int val6 = rs.getInt("val6");
            String val1 = rs.getString("val1");
            String val2 = rs.getString("val2");

            System.out.print("val5: " + val5);
            System.out.print(", val6: " + val6);
            System.out.print(", val1: " + val1);
            System.out.println(", val2: " + val2);
        }

        stmt.execute("DISABLE TABLE table12");
        stmt.execute("DROP TABLE table12");

        // END SNIPPET: jdbc1
    }

    @Test
    public void annotatedSelect() throws HBqlException {

        // START SNIPPET: annotatedExample2

        // Get a connection to HBase
        HConnection connection = HConnectionManager.newConnection();

        connection.execute("CREATE TEMP SCHEMA demo2 FOR TABLE example2"
                           + "("
                           + "keyval KEY, "
                           + "f1 ("
                           + "  val1 STRING ALIAS val1, "
                           + "  val2 INT ALIAS val2, "
                           + "  val3 STRING ALIAS val3 DEFAULT 'This is a default value' "
                           + "))");

        // Clean up table
        if (!connection.tableExists("example2"))
            System.out.println(connection.execute("CREATE TABLE example2 (f1)"));
        else
            System.out.println(connection.execute("DELETE FROM demo2"));

        // Add some records using an INSERT stmt
        HPreparedStatement stmt = connection.prepareStatement("INSERT INTO demo2 " +
                                                              "(keyval, val1, val2, val3) VALUES " +
                                                              "(ZEROPAD(:key, 10), :val1, :val2, DEFAULT)");

        for (int i = 0; i < 5; i++) {
            stmt.setParameter("key", i);
            stmt.setParameter("val1", "Value: " + i);
            stmt.setParameter("val2", i);
            stmt.execute();
        }

        // Add some other records using an AnnotatedExample object
        final HBatch batch = new HBatch(connection);
        for (int i = 5; i < 10; i++) {
            AnnotatedExample obj = new AnnotatedExample();
            obj.keyval = Util.getZeroPaddedNumber(i, 10);
            obj.val1 = "Value: " + i;
            obj.val2 = i;
            batch.insert(obj);
        }
        batch.apply();

        // Query the records just added
        HResultSet<AnnotatedExample> records = connection.executeQuery("SELECT * FROM demo2", AnnotatedExample.class);

        for (AnnotatedExample rec : records) {
            System.out.println("Key = " + rec.keyval);
            System.out.println("f1:val1 = " + rec.val1);
            System.out.println("f1:val2 = " + rec.val2);
            System.out.println("f1:val3 = " + rec.val3);
        }

        // END SNIPPET: annotatedExample2
    }
}