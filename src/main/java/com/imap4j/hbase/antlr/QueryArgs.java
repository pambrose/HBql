package com.imap4j.hbase.antlr;

import com.imap4j.hbase.hql.expr.CondExpr;

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
    private final CondExpr whereExpr;

    public QueryArgs(final List<String> columnList, final String tableName, final CondExpr whereExpr) {
        this.tableName = tableName;
        this.columnList = columnList;
        this.whereExpr = whereExpr;
    }

    public List<String> getColumnList() {
        return columnList;
    }

    public String getTableName() {
        return tableName;
    }

    public CondExpr getWhereExpr() {
        return whereExpr;
    }
}
