package org.apache.hadoop.hbase.hbql.query.antlr.cmds;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 24, 2009
 * Time: 10:31:14 PM
 */
public class DropCmd extends TableCmd implements ConnectionCmd {

    public DropCmd(final String tableName) {
        super(tableName);
    }

    @Override
    public HOutput execute(final HConnection conn) throws HPersistException, IOException {

        final HBaseSchema schema = HBaseSchema.findSchema(this.getTableName());

        final HBaseAdmin admin = new HBaseAdmin(conn.getConfig());
        admin.deleteTable(schema.getTableName());

        final HOutput retval = new HOutput();
        retval.out.println("Table " + schema.getTableName() + " dropped.");
        retval.out.flush();
        return retval;
    }

}