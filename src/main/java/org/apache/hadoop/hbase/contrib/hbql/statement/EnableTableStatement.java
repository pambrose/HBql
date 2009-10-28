package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;

import java.io.IOException;

public class EnableTableStatement extends TableStatement implements ConnectionStatement {

    public EnableTableStatement(final String tableName) {
        super(tableName);
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {
        conn.getAdmin().enableTable(this.getTableName());
        return new HOutput("Table " + this.getTableName() + " enabled.");
    }
}