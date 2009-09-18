package org.apache.hadoop.hbase.hbql.query.antlr.cmds;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
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
public class CreateCmd extends TableCmd implements ConnectionExecCmd {

    public CreateCmd(final String tableName) {
        super(tableName);
    }

    public HOutput exec(final HConnection conn) throws HPersistException, IOException {

        final HBaseSchema schema = HBaseSchema.findSchema(this.getTableName());

        final HTableDescriptor tableDesc = new HTableDescriptor(schema.getTableName());

        for (final HColumnDescriptor columnDesc : schema.getColumnDescriptors())
            tableDesc.addFamily(columnDesc);

        final HBaseAdmin admin = new HBaseAdmin(conn.getConfig());

        admin.createTable(tableDesc);

        final HOutput retval = new HOutput();
        retval.out.println("Table " + tableDesc.getNameAsString() + " created.");
        retval.out.flush();
        return retval;
    }

}
