package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.hbql.query.stmt.args.QueryArgs;

public class SelectCmd implements ShellCommand {

    private final QueryArgs queryArgs;

    public SelectCmd(final QueryArgs queryArgs) {
        this.queryArgs = queryArgs;
    }
}
