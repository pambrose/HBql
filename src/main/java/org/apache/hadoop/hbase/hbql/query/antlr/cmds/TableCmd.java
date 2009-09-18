package org.apache.hadoop.hbase.hbql.query.antlr.cmds;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 17, 2009
 * Time: 8:03:09 PM
 */
public abstract class TableCmd implements ExecCmd {

    private final String tableName;

    protected TableCmd(final String tableName) {
        this.tableName = tableName;
    }

    protected String getTableName() {
        return tableName;
    }
}
