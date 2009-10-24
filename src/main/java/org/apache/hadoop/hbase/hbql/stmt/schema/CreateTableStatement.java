package org.apache.hadoop.hbase.hbql.stmt.schema;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.stmt.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.stmt.SchemaStatement;

import java.io.IOException;

public class CreateTableStatement extends SchemaStatement implements ConnectionStatement {

    public CreateTableStatement(final String schemaName) {
        super(schemaName);
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        final HTableDescriptor tableDesc = new HTableDescriptor(this.getSchema().getTableName());

        for (final HColumnDescriptor columnDesc : this.getSchema().getColumnDescriptors())
            tableDesc.addFamily(columnDesc);

        conn.getAdmin().createTable(tableDesc);

        final HOutput retval = new HOutput();
        retval.out.println("Table " + tableDesc.getNameAsString() + " created.");
        retval.out.flush();
        return retval;
    }
}
