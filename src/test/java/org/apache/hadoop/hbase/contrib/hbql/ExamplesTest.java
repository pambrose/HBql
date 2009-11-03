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
        conn.execute("CREATE TABLE USING SCHEMA foo_schema");

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
}