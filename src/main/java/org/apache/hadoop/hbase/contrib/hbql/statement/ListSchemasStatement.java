package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;
import org.apache.hadoop.hbase.contrib.hbql.client.HSchemaManager;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;

import java.io.IOException;

public class ListSchemasStatement implements ConnectionStatement {

    public ListSchemasStatement() {
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        final HOutput retval = new HOutput();
        retval.out.println("Schema names: ");
        for (final String schemaName : HSchemaManager.getDefinedSchemaList())
            retval.out.println("\t" + schemaName);

        retval.out.flush();
        return retval;
    }
}