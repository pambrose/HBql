package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.util.TestSupport;

import java.io.IOException;

public class ExamplesTest extends TestSupport {

    public void showTable() throws HBqlException, IOException {

        // START SNIPPET: show-tables
        HConnection conn = HConnectionManager.newHConnection();
        System.out.println(conn.execute("SHOW TABLES"));
        // END SNIPPET: show-tables
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

        // START SNIPPET: drop_schema
        System.out.println(SchemaManager.execute("DROP SCHEMA foo_schema"));
        // END SNIPPET: drop_schema

    }


    public void defineSchema() throws HBqlException, IOException {

        // START SNIPPET: define-schema1
        // Schema named foo that corresponds to table foo.
        SchemaManager.execute("DEFINE SCHEMA foo (keyval key, family1:val1 STRING)");
        // END SNIPPET: define-schema1

        // START SNIPPET: define-schema2
        // Schema named schema1 that corresponds to table foo.
        SchemaManager.execute("DEFINE SCHEMA schema1 FOR TABLE foo (keyval key, family1:val1 STRING ALIAS val2)");
        // END SNIPPET: define-schema2

        // START SNIPPET: define-schema3
        // A column with a default value.
        SchemaManager.execute("DEFINE SCHEMA schema1 FOR TABLE foo "
                              + "("
                              + "keyval key, "
                              + "family1:val1 STRING ALIAS val2 DEFAULT 'this is a default value'"
                              + ")");
        // END SNIPPET: define-schema3

        // START SNIPPET: define-schema4
        // A schema with a family default attribute.
        SchemaManager.execute("DEFINE SCHEMA schema1 FOR TABLE foo "
                              + "("
                              + "keyval key, "
                              + "family1:val1 STRING ALIAS val2, "
                              + "family1:* ALIAS family1_default"
                              + ")");
        // END SNIPPET: define-schema4

    }
}