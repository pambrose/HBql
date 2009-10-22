package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;

import java.io.IOException;

public class DisableTableCmd extends TableCmd implements ConnectionCmd {

    public DisableTableCmd(final String tableName) {
        super(tableName);
    }

    public HOutput execute(final HConnection conn) throws HBqlException, IOException {

        final HBaseSchema schema = HBaseSchema.findSchema(this.getTableName());

        final HBaseAdmin admin = new HBaseAdmin(conn.getConfig());
        admin.disableTable(schema.getTableName());

        final HOutput retval = new HOutput();
        retval.out.println("Table " + schema.getTableName() + " disabled.");
        retval.out.flush();
        return retval;
    }
}