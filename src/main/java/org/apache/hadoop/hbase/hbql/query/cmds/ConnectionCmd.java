package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HOutput;

import java.io.IOException;

public interface ConnectionCmd extends ShellCommand {

    public HOutput execute(final HConnection conn) throws HBqlException, IOException;
}
