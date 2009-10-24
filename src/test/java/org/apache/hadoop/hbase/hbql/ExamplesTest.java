package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.util.TestSupport;

import java.io.IOException;

public class ExamplesTest extends TestSupport {

    public void showTable() throws HBqlException, IOException {

        // START SNIPPET: list-tables
        HConnection conn = HConnectionManager.newHConnection();
        System.out.println(conn.execute("LIST TABLES"));
        // END SNIPPET: list-tables
    }

    public void describeTable() throws HBqlException, IOException {

        // START SNIPPET: describe-table
        HConnection conn = HConnectionManager.newHConnection();
        System.out.println(conn.execute("DESCRIBE TABLE foo"));
        // END SNIPPET: describe-table

    }

    public void enableTable() throws HBqlException, IOException {

        // START SNIPPET: enable-table
        HConnection conn = HConnectionManager.newHConnection();
        System.out.println(conn.execute("ENABLE TABLE foo"));
        // END SNIPPET: enable-table

    }

    public void disableTable() throws HBqlException, IOException {

        // START SNIPPET: disable-table
        HConnection conn = HConnectionManager.newHConnection();
        System.out.println(conn.execute("DISABLE TABLE foo"));
        // END SNIPPET: disable-table

    }

    public void dropSchema() throws HBqlException, IOException {

        // START SNIPPET: drop-schema
        System.out.println(SchemaManager.execute("DROP SCHEMA foo_schema"));
        // END SNIPPET: drop-schema

    }

    public void createTable() throws HBqlException, IOException {

        // START SNIPPET: create-table
        HConnection conn = HConnectionManager.newHConnection();
        System.out.println(conn.execute("CREATE TABLE USING SCHEMA foo_schema"));
        // END SNIPPET: create-table

    }

    public void dropTable() throws HBqlException, IOException {

        // START SNIPPET: drop-table
        HConnection conn = HConnectionManager.newHConnection();
        System.out.println(conn.execute("DROP TABLE foo"));
        // END SNIPPET: drop-table

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
                              + "family1:val1 STRING ALIAS val2 DEFAULT 'this is a default value'"
                              + ")");
        // END SNIPPET: create-schema3

        // START SNIPPET: create-schema4
        // A schema with a family default attribute.
        SchemaManager.execute("CREATE SCHEMA schema1 FOR TABLE foo "
                              + "("
                              + "keyval key, "
                              + "family1:val1 STRING ALIAS val2, "
                              + "family1:* ALIAS family1_default"
                              + ")");
        // END SNIPPET: create-schema4

    }
}