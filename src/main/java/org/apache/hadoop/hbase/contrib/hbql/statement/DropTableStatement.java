package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.Output;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;

import java.io.IOException;

public class DropTableStatement extends TableStatement {

    public DropTableStatement(final String tableName) {
        super(tableName);
    }

    public Output execute(final ConnectionImpl conn) throws HBqlException, IOException {
        conn.dropTable(this.getTableName());
        return new Output("Table " + this.getTableName() + " dropped.");
    }
}