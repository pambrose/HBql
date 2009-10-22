package org.apache.hadoop.hbase.hbql.query.cmds.table;

public abstract class TableCommand {

    private final String tableName;

    protected TableCommand(final String tableName) {
        this.tableName = tableName;
    }

    protected String getTableName() {
        return tableName;
    }
}
