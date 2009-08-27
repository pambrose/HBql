package com.imap4j.hbase.antlr;

import com.imap4j.hbase.hql.expr.WhereExpr;

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
    private final WhereExpr whereExpr;

    public QueryArgs(final List<String> columnList, final String tableName, final WhereExpr whereExpr) {
        this.tableName = tableName;
        this.columnList = columnList;
        this.whereExpr = whereExpr;
    }

    public List<String> getColumnList() {
        return this.columnList;
    }

    public String getTableName() {
        return this.tableName;
    }

    public WhereExpr getWhereExpr() {
        if (this.whereExpr != null)
            return this.whereExpr;
        else
            return new WhereExpr(null);
    }
}
