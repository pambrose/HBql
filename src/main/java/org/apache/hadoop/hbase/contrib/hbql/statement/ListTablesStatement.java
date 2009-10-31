package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.contrib.hbql.client.Output;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;

import java.io.IOException;

public class ListTablesStatement implements ConnectionStatement {

    public ListTablesStatement() {
    }

    public Output execute(final ConnectionImpl conn) throws HBqlException, IOException {

        final HBaseAdmin admin = conn.getAdmin();

        final Output retval = new Output();
        retval.out.println("Tables: ");
        for (final HTableDescriptor tableDesc : admin.listTables())
            retval.out.println("\t" + tableDesc.getNameAsString());

        retval.out.flush();
        return retval;
    }
}