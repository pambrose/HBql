package com.imap4j.hbase.antlr.args;

import com.imap4j.hbase.hbql.expr.predicate.WhereExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 11:43:49 PM
 */
public class DeleteArgs implements ExecArgs {

    private final String tableName;
    private final WhereExpr whereExpr;

    public DeleteArgs(final String tableName, final WhereExpr whereExpr) {
        this.tableName = tableName;
        this.whereExpr = whereExpr;
    }

    public String getTableName() {
        return this.tableName;
    }

    public WhereExpr getWhereExpr() {
        return this.whereExpr;
    }
}
