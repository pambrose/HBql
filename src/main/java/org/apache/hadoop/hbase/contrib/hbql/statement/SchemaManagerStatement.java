package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.Output;

public interface SchemaManagerStatement extends ShellStatement {

    public Output execute() throws HBqlException;
}