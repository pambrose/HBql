package org.apache.hadoop.hbase.hbql.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;

public interface SchemaManagerStatement extends ShellStatement {

    public HOutput execute() throws HBqlException;
}