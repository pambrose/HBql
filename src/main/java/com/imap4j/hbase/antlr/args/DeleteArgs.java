package com.imap4j.hbase.antlr.args;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 11:43:49 PM
 */
public class DeleteArgs implements ExecArgs {

    private final String tableName;
    private final WhereArgs whereExpr;

    public DeleteArgs(final String tableName, final WhereArgs whereExpr) {
        this.tableName = tableName;
        this.whereExpr = whereExpr;
    }

    public String getTableName() {
        return this.tableName;
    }

    public WhereArgs getWhereExpr() {
        if (whereExpr == null)
            return new WhereArgs();
        else
            return this.whereExpr;
    }

}
