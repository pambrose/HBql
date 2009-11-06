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

package org.apache.hadoop.hbase.contrib.hbql;

import org.apache.hadoop.hbase.contrib.hbql.client.Batch;
import org.apache.hadoop.hbase.contrib.hbql.client.Connection;
import org.apache.hadoop.hbase.contrib.hbql.client.ConnectionManager;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.contrib.hbql.client.Query;
import org.apache.hadoop.hbase.contrib.hbql.client.Record;
import org.apache.hadoop.hbase.contrib.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.contrib.hbql.client.Util;
import org.apache.hadoop.hbase.contrib.hbql.util.TestSupport;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class ExamplesTest extends TestSupport {


    public void showTables() throws HBqlException, IOException {

        // START SNIPPET: list-tables

        Connection conn = ConnectionManager.newConnection();
        System.out.println(conn.execute("LIST TABLES"));

        // Or using the API
        Set<String> tableNamess = conn.getTableNames();

        // END SNIPPET: list-tables
    }

    public void showSchemas() throws HBqlException, IOException {

        // START SNIPPET: list-schemas

        System.out.println(SchemaManager.execute("LIST SCHEMAS"));

        // Or using the API
        Set<String> schemaNamess = SchemaManager.getDefinedSchemaNames();

        // END SNIPPET: list-schemas
    }

    public void describeTable() throws HBqlException, IOException {

        // START SNIPPET: describe-table

        Connection conn = ConnectionManager.newConnection();
        System.out.println(conn.execute("DESCRIBE TABLE foo"));

        // END SNIPPET: describe-table

    }

    public void describeSchema() throws HBqlException, IOException {

        // START SNIPPET: describe-schema

        Connection conn = ConnectionManager.newConnection();
        System.out.println(conn.execute("DESCRIBE SCHEMA foo_schema"));

        // END SNIPPET: describe-schema

    }

    public void enableTable() throws HBqlException, IOException {

        // START SNIPPET: enable-table

        Connection conn = ConnectionManager.newConnection();
        System.out.println(conn.execute("ENABLE TABLE foo"));

        // Or using the API
        conn.enableTable("foo");

        // END SNIPPET: enable-table

    }

    public void disableTable() throws HBqlException, IOException {

        // START SNIPPET: disable-table

        Connection conn = ConnectionManager.newConnection();
        conn.execute("DISABLE TABLE foo");

        // Or using the API
        conn.disableTable("foo");

        // END SNIPPET: disable-table

    }

    public void dropSchema() throws HBqlException, IOException {

        // START SNIPPET: drop-schema

        SchemaManager.execute("DROP SCHEMA foo_schema");

        // Or using the API
        SchemaManager.dropSchema("foo_schema");

        // END SNIPPET: drop-schema

    }

    public void createTable() throws HBqlException, IOException {

        // START SNIPPET: create-table

        Connection conn = ConnectionManager.newConnection();
        conn.execute("CREATE TABLE USING foo_schema");

        // END SNIPPET: create-table

    }

    public void dropTable() throws HBqlException, IOException {

        // START SNIPPET: drop-table

        Connection conn = ConnectionManager.newConnection();
        conn.execute("DROP TABLE foo");

        // Or using the API
        conn.dropTable("foo");

        // END SNIPPET: drop-table

    }

    public void insert1() throws HBqlException, IOException {

        // START SNIPPET: insert1

        SchemaManager.execute("CREATE SCHEMA foo_schema FOR TABLE foo "
                              + "("
                              + "keyval KEY, "
                              + "family1:val1 INT ALIAS val1, "
                              + "family1:val2 STRING ALIAS val2"
                              + ")");

        Connection conn = ConnectionManager.newConnection();

        System.out.println(conn.execute("INSERT INTO foo_schema (keyval, val1, val2) "
                                        + "VALUES (ZEROPAD(2, 10), 123, 'test val')"));

        // Or using the Record interface
        Record rec = SchemaManager.newRecord("foo_schema");
        rec.setCurrentValue("keyval", Util.getZeroPaddedNumber(2, 10));
        rec.setCurrentValue("val1", 123);
        rec.setCurrentValue("al2", "testval");

        Batch batch = new Batch();
        batch.insert(rec);

        conn.apply(batch);

        // END SNIPPET: insert1

    }

    public void insert2() throws HBqlException, IOException {

        // START SNIPPET: insert2

        // A column with a default value.
        SchemaManager.execute("CREATE SCHEMA foo_schema FOR TABLE foo "
                              + "("
                              + "keyval KEY, "
                              + "family1:val1 INT ALIAS val1, "
                              + "family1:val2 STRING ALIAS val2 DEFAULT 'this is a default value'"
                              + ")");

        Connection conn = ConnectionManager.newConnection();
        PreparedStatement ps = conn.prepare("INSERT INTO foo_schema (keyval, val1, val2) "
                                            + "VALUES (:key, :val1, DEFAULT)");

        ps.setParameter("key", Util.getZeroPaddedNumber(2, 10));
        ps.setParameter("val1", 123);

        System.out.println(ps.execute());
        // END SNIPPET: insert2

    }

    public void insert3() throws HBqlException, IOException {

        // START SNIPPET: insert3
        SchemaManager.execute("CREATE SCHEMA foo_schema FOR TABLE foo "
                              + "("
                              + "keyval KEY, "
                              + "family1:val1 STRING ALIAS val1, "
                              + "family1:val2 STRING ALIAS val2, "
                              + "family1:val3 STRING ALIAS val3, "
                              + "family1:val4 STRING ALIAS val4 "
                              + ")");
        Connection conn = ConnectionManager.newConnection();

        System.out.println(conn.execute("INSERT INTO foo_schema (keyval, val1, val2) "
                                        + "SELECT keyval, val3, val4 FROM foo2_schema"));
        // END SNIPPET: insert3

    }


    public void createSchema() throws HBqlException, IOException {

        // START SNIPPET: create-schema1
        // Schema named foo that corresponds to table foo.
        SchemaManager.execute("CREATE SCHEMA foo (keyval key, family1:val1 STRING)");
        // END SNIPPET: create-schema1

        // START SNIPPET: create-schema2
        // Schema named schema1 that corresponds to table foo.
        SchemaManager.execute("CREATE SCHEMA schema1 FOR TABLE foo (keyval key, family1:val1 STRING ALIAS val2)");
        // END SNIPPET: create-schema2

        // START SNIPPET: create-schema3
        // A column with a default value.
        SchemaManager.execute("CREATE SCHEMA schema1 FOR TABLE foo "
                              + "("
                              + "keyval key, "
                              + "family1:val1 STRING ALIAS val1 DEFAULT 'this is a default value'"
                              + ")");
        // END SNIPPET: create-schema3

        // START SNIPPET: create-schema4

        // A schema with a family default attribute.
        SchemaManager.execute("CREATE SCHEMA schema1 FOR TABLE foo "
                              + "("
                              + "keyval key, "
                              + "family1:val1 STRING ALIAS val1, "
                              + "family1:* ALIAS family1_default"
                              + ")");

        // END SNIPPET: create-schema4

    }

    public void selectAll() throws HBqlException, IOException {

        // START SNIPPET: select1

        Connection conn = ConnectionManager.newConnection();

        SchemaManager.execute("CREATE SCHEMA tab1 FOR TABLE table1"
                              + "("
                              + "keyval KEY, "
                              + "f1:val1 STRING ALIAS val1, "
                              + "f3:val1 INT ALIAS val5, "
                              + "f3:val2 INT ALIAS val6, "
                              + "f3:val3 INT ALIAS val7, "
                              + "f1:* ALIAS f1default, "
                              + "f2:* ALIAS f2default, "
                              + "f3:* ALIAS f3default "
                              + ")");

        Query<Record> q1 = conn.newQuery("SELECT keyval, f1:val1, val5 FROM tab1 "
                                         + "WITH KEYS FIRST TO :endkey "
                                         + "VERSIONS 4 "
                                         + "CLIENT FILTER WHERE val6 > 4");

        q1.setParameter("endkey", Util.getZeroPaddedNumber(34, 10));

        for (Record record : q1.getResults()) {
            System.out.println("Key = " + record.getCurrentValue("keyval"));
        }

        // END SNIPPET: select1
    }

    @Test
    public void definedSelect() throws HBqlException, IOException {

        // START SNIPPET: definedExample1

        // Create schema
        SchemaManager.execute("CREATE SCHEMA demo1 FOR TABLE example1"
                              + "("
                              + "keyval KEY, "
                              + "f1:val1 STRING ALIAS val1, "
                              + "f1:val2 INT ALIAS val2, "
                              + "f1:val3 STRING DEFAULT 'This is a default value' "
                              + ")");

        // Get Connection to HBase
        Connection conn = ConnectionManager.newConnection();

        // Clean up table
        if (!conn.tableExists("example1"))
            System.out.println(conn.execute("CREATE TABLE USING demo1"));
        else
            System.out.println(conn.execute("DELETE FROM demo1"));

        // Add some records using an INSERT stmt
        PreparedStatement stmt = conn.prepare("INSERT INTO demo1 " +
                                              "(keyval, val1, val2, f1:val3) VALUES " +
                                              "(ZEROPAD(:key, 10), :val1, :val2, DEFAULT)");

        for (int i = 0; i < 5; i++) {
            stmt.setParameter("key", i);
            stmt.setParameter("val1", "Value: " + i);
            stmt.setParameter("val2", i);
            stmt.execute();
        }

        // Add some other records using the Record interface
        final Batch batch = new Batch();
        for (int i = 5; i < 10; i++) {
            Record rec = SchemaManager.newRecord("demo1");
            rec.setCurrentValue("keyval", Util.getZeroPaddedNumber(i, 10));
            rec.setCurrentValue("val1", "Value: " + i);
            rec.setCurrentValue("f1:val2", i);
            batch.insert(rec);
        }
        conn.apply(batch);

        // Query the records just added
        Query<Record> query = conn.newQuery("SELECT * FROM demo1");

        for (Record rec : query.getResults()) {
            System.out.println("Key = " + rec.getCurrentValue("keyval"));
            System.out.println("f1:val1 = " + rec.getCurrentValue("val1"));
            System.out.println("f1:val2 = " + rec.getCurrentValue("f1:val2"));
            System.out.println("f1:val3 = " + rec.getCurrentValue("f1:val3"));
        }

        // END SNIPPET: definedExample1
    }

    @Test
    public void annotatedSelect() throws HBqlException, IOException {

        // START SNIPPET: annotatedExample2

        // Get Connection to HBase
        Connection conn = ConnectionManager.newConnection();

        // Clean up table
        if (!conn.tableExists("example2"))
            System.out.println(conn.execute("CREATE TABLE USING AnnotatedExample"));
        else
            System.out.println(conn.execute("DELETE FROM AnnotatedExample"));

        // Add some records using an INSERT stmt
        PreparedStatement stmt = conn.prepare("INSERT INTO AnnotatedExample " +
                                              "(keyval, val1, val2, val3) VALUES " +
                                              "(ZEROPAD(:key, 10), :val1, :val2, DEFAULT)");

        for (int i = 0; i < 5; i++) {
            stmt.setParameter("key", i);
            stmt.setParameter("val1", "Value: " + i);
            stmt.setParameter("val2", i);
            stmt.execute();
        }

        // Add some other records using an AnnotatedExample object
        final Batch batch = new Batch();
        for (int i = 5; i < 10; i++) {
            AnnotatedExample obj = new AnnotatedExample();
            obj.keyval = Util.getZeroPaddedNumber(i, 10);
            obj.val1 = "Value: " + i;
            obj.val2 = i;
            batch.insert(obj);
        }
        conn.apply(batch);

        // Query the records just added
        Query<AnnotatedExample> query = conn.newQuery("SELECT * FROM AnnotatedExample");

        for (AnnotatedExample rec : query.getResults()) {
            System.out.println("Key = " + rec.keyval);
            System.out.println("f1:val1 = " + rec.val1);
            System.out.println("f1:val2 = " + rec.val2);
            System.out.println("f1:val3 = " + rec.val3);
        }

        // END SNIPPET: annotatedExample2
    }
}