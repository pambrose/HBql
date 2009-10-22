package org.apache.hadoop.hbase.hbql.query.cmds.schema;

import org.apache.hadoop.hbase.hbql.query.cmds.ShellCommand;
import org.apache.hadoop.hbase.hbql.query.stmt.args.QueryArgs;

public class SelectRecords implements ShellCommand {

    private final QueryArgs queryArgs;

    public SelectRecords(final QueryArgs queryArgs) {
        this.queryArgs = queryArgs;
    }
}
