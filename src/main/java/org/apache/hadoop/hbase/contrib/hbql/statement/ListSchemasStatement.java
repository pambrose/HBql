package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.Output;
import org.apache.hadoop.hbase.contrib.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;

import java.io.IOException;

public class ListSchemasStatement implements ConnectionStatement {

    public ListSchemasStatement() {
    }

    public Output execute(final ConnectionImpl conn) throws HBqlException, IOException {

        final Output retval = new Output();
        retval.out.println("Schemas: ");
        for (final String schemaName : SchemaManager.getDefinedSchemaList())
            retval.out.println("\t" + schemaName);

        retval.out.flush();
        return retval;
    }
}