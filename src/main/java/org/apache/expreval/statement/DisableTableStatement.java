package org.apache.expreval.statement;

import org.apache.expreval.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;

import java.io.IOException;

public class DisableTableStatement extends TableStatement implements ConnectionStatement {

    public DisableTableStatement(final String tableName) {
        super(tableName);
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {
        conn.getAdmin().disableTable(this.getTableName());
        return new HOutput("Table " + this.getTableName() + " disabled.");
    }
}