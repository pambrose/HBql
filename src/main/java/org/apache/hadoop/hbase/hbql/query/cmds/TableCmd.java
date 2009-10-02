package org.apache.hadoop.hbase.hbql.query.cmds;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 17, 2009
 * Time: 8:03:09 PM
 */
public abstract class TableCmd {

    private final String tableName;

    protected TableCmd(final String tableName) {
        this.tableName = tableName;
    }

    protected String getTableName() {
        return tableName;
    }
}
