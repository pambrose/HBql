package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;

public interface SchemaManagerStatement extends ShellStatement {

    public HOutput execute() throws HBqlException;
}