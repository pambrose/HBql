package org.apache.expreval.statement;

import org.apache.expreval.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;

import java.io.IOException;

public class DropTableStatement extends TableStatement implements ConnectionStatement {

    public DropTableStatement(final String tableName) {
        super(tableName);
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {
        conn.getAdmin().deleteTable(this.getTableName());
        return new HOutput("Table " + this.getTableName() + " dropped.");
    }
}