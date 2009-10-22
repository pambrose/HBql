package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;

import java.io.IOException;

public class CreateTableCmd extends SchemaCmd implements ConnectionCmd {

    public CreateTableCmd(final String schemaName) {
        super(schemaName);
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        final HBaseSchema schema = HBaseSchema.findSchema(this.getSchemaName());

        final HTableDescriptor tableDesc = new HTableDescriptor(schema.getTableName());

        for (final HColumnDescriptor columnDesc : schema.getColumnDescriptors())
            tableDesc.addFamily(columnDesc);

        conn.getAdmin().createTable(tableDesc);

        final HOutput retval = new HOutput();
        retval.out.println("Table " + tableDesc.getNameAsString() + " created.");
        retval.out.flush();
        return retval;
    }
}
