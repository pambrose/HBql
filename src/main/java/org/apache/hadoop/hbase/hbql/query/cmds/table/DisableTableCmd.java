package org.apache.hadoop.hbase.hbql.query.cmds.table;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.cmds.ConnectionCmd;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;

import java.io.IOException;

public class DisableTableCmd extends TableCmd implements ConnectionCmd {

    public DisableTableCmd(final String tableName) {
        super(tableName);
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        conn.getAdmin().disableTable(this.getTableName());

        final HOutput retval = new HOutput();
        retval.out.println("Table " + this.getTableName() + " disabled.");
        retval.out.flush();
        return retval;
    }
}