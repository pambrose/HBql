package org.apache.hadoop.hbase.hbql.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;

import java.io.IOException;

public interface ConnectionStatement extends ShellStatement {

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException;
}
