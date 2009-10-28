package org.apache.expreval.statement;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;

public interface SchemaManagerStatement extends ShellStatement {

    public HOutput execute() throws HBqlException;
}