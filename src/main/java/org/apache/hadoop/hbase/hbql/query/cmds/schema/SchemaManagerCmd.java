package org.apache.hadoop.hbase.hbql.query.cmds.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.cmds.ShellCommand;

public interface SchemaManagerCmd extends ShellCommand {

    public HOutput execute() throws HBqlException;
}