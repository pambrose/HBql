package org.apache.hadoop.hbase.hbql.query.cmds.table;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.cmds.ConnectionCmd;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;

import java.io.IOException;

public class EnableTableCmd extends TableCmd implements ConnectionCmd {

    public EnableTableCmd(final String tableName) {
        super(tableName);
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        conn.getAdmin().enableTable(this.getTableName());

        final HOutput retval = new HOutput();
        retval.out.println("Table " + this.getTableName() + " enabled.");
        retval.out.flush();
        return retval;
    }
}