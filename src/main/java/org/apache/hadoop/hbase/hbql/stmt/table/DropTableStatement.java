package org.apache.hadoop.hbase.hbql.stmt.table;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.stmt.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.stmt.TableStatement;

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