package com.imap4j.hbase.antlr.args;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 11:07:28 PM
 */
public class QueryArgs {

    private final List<String> columnList;
    private final String tableName;
    private final WhereArgs whereExpr;

    public QueryArgs(final List<String> columnList, final String tableName, final WhereArgs whereExpr) {
        this.tableName = tableName;
        this.columnList = columnList;
        this.whereExpr = whereExpr;
    }

    public List<String> getColumns() {
        return this.columnList;
    }

    public String getTableName() {
        return this.tableName;
    }

    public WhereArgs getWhereExpr() {
        if (this.whereExpr != null)
            return this.whereExpr;
        else
            return new WhereArgs();
    }
}
