package org.apache.expreval.statement;

import org.apache.expreval.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;

import java.io.IOException;

public interface ConnectionStatement extends ShellStatement {

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException;
}
