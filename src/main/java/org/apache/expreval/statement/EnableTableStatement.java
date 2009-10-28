package org.apache.expreval.statement;

import org.apache.expreval.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;

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