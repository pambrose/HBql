package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.Output;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;

import java.io.IOException;

public class DisableTableStatement extends TableStatement {

    public DisableTableStatement(final String tableName) {
        super(tableName);
    }

    public Output execute(final ConnectionImpl conn) throws HBqlException, IOException {
        conn.getAdmin().disableTable(this.getTableName());
        return new Output("Table " + this.getTableName() + " disabled.");
    }
}