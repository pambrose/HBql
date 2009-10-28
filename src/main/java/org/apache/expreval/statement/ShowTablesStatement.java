package org.apache.expreval.statement;

import org.apache.expreval.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;

import java.io.IOException;

public class ShowTablesStatement implements ConnectionStatement {

    public ShowTablesStatement() {
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        final HBaseAdmin admin = conn.getAdmin();

        final HOutput retval = new HOutput();
        retval.out.println("Table names: ");
        for (final HTableDescriptor tableDesc : admin.listTables())
            retval.out.println("\t" + tableDesc.getNameAsString());

        retval.out.flush();
        return retval;
    }
}