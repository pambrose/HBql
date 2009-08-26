package com.imap4j.hbase.antlr;

import com.imap4j.hbase.hql.expr.OrExpr;

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
    private final OrExpr whereExpr;

    public QueryArgs(final List<String> columnList, final String tableName, final OrExpr whereExpr) {
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

    public OrExpr getWhereExpr() {
        return whereExpr;
    }
}
