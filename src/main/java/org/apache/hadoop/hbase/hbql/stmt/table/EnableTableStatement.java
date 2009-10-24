package org.apache.hadoop.hbase.hbql.stmt.table;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.stmt.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.stmt.TableStatement;

import java.io.IOException;

public class EnableTableStatement extends TableStatement implements ConnectionStatement {

    public EnableTableStatement(final String tableName) {
        super(tableName);
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        conn.getAdmin().enableTable(this.getTableName());

        final HOutput retval = new HOutput();
        retval.out.println("Table " + this.getTableName() + " enabled.");
        retval.out.flush();
        return retval;
    }
}