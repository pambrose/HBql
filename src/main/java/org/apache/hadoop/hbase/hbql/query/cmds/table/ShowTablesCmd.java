package org.apache.hadoop.hbase.hbql.query.cmds.table;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.cmds.ConnectionCmd;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;

import java.io.IOException;

public class ShowTablesCmd implements ConnectionCmd {

    public ShowTablesCmd() {
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