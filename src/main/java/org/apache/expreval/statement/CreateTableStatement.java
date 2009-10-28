package org.apache.expreval.statement;

import org.apache.expreval.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;

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

        return new HOutput("Table " + tableDesc.getNameAsString() + " created.");
    }
}
