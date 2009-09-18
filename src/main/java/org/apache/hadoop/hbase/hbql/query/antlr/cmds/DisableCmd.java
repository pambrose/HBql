package org.apache.hadoop.hbase.hbql.query.antlr.cmds;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.HPersistException;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 24, 2009
 * Time: 10:31:14 PM
 */
public class DisableCmd extends TableCmd {

    public DisableCmd(final String tableName) {
        super(tableName);
    }

    @Override
    public HOutput exec(final HConnection conn) throws HPersistException, IOException {
        final HBaseAdmin admin = new HBaseAdmin(conn.getConfig());
        admin.disableTable(this.getTableName());

        final HOutput retval = new HOutput();
        retval.out.println("Table " + this.getTableName() + " disabled.");
        retval.out.flush();
        return retval;
    }

}