package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;

import java.io.IOException;

public class HelpStatement implements ConnectionStatement {

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        final HOutput retval = new HOutput();
        retval.out.println("VERSION");
        retval.out.println("LIST TABLES");
        retval.out.println("DESCRIBE TABLE table_name");
        retval.out.flush();

        return retval;
    }
}