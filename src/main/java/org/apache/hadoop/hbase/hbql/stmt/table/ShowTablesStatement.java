package org.apache.hadoop.hbase.hbql.stmt.table;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.stmt.ConnectionStatement;

import java.io.IOException;

public class ShowTablesStatement implements ConnectionStatement {

    public ShowTablesStatement() {
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        final HOutput retval = new HOutput();

        final HBaseAdmin admin = conn.getAdmin();

        retval.out.println("Table names: ");
        for (final HTableDescriptor tableDesc : admin.listTables())
            retval.out.println("\t" + tableDesc.getNameAsString());

        retval.out.flush();
        return retval;
    }
}