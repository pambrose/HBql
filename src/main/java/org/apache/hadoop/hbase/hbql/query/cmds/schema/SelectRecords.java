package org.apache.hadoop.hbase.hbql.query.cmds.schema;

import org.apache.hadoop.hbase.hbql.query.cmds.ShellCommand;
import org.apache.hadoop.hbase.hbql.query.stmt.args.SelectStmt;

public class SelectRecords implements ShellCommand {

    private final SelectStmt selectStmt;

    public SelectRecords(final SelectStmt selectStmt) {
        this.selectStmt = selectStmt;
    }

    public SelectStmt getQueryArgs() {
        return this.selectStmt;
    }
}
