package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.Output;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;

import java.io.IOException;

public class HelpStatement implements ConnectionStatement {

    public Output execute(final ConnectionImpl conn) throws HBqlException, IOException {

        final Output retval = new Output();
        retval.out.println("VERSION");
        retval.out.println("LIST TABLES");
        retval.out.println("DESCRIBE TABLE table_name");
        retval.out.flush();

        return retval;
    }
}