package org.apache.hadoop.hbase.hbql.query.cmds.record;

import org.apache.hadoop.hbase.hbql.query.cmds.ShellCommand;
import org.apache.hadoop.hbase.hbql.query.stmt.args.QueryArgs;

public class SelectRecordsCmd implements ShellCommand {

    private final QueryArgs queryArgs;

    public SelectRecordsCmd(final QueryArgs queryArgs) {
        this.queryArgs = queryArgs;
    }
}
