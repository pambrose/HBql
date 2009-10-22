package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;

import java.io.IOException;

public class CreateTableCmd extends TableCmd implements ConnectionCmd {

    public CreateTableCmd(final String tableName) {
        super(tableName);
    }

    public HOutput execute(final HConnection conn) throws HBqlException, IOException {

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
