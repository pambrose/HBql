package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;

public interface SchemaManagerCommand extends ShellCommand {

    public HOutput execute() throws HBqlException;
}