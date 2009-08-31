package com.imap4j.hbase.antlr.args;

import com.imap4j.hbase.hbql.expr.predicate.ExprEvalTree;

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
    private final ExprEvalTree filterExpr;
    private final ExprEvalTree whereExpr;

    public QueryArgs(final List<String> columnList, final String tableName, final ExprEvalTree filterExpr, final ExprEvalTree whereExpr) {
        this.tableName = tableName;
        this.columnList = columnList;
        this.filterExpr = filterExpr;
        this.whereExpr = whereExpr;
    }

    public List<String> getColumnList() {
        return this.columnList;
    }

    public String getTableName() {
        return this.tableName;
    }

    public ExprEvalTree getFilterExpr() {
        if (this.filterExpr != null)
            return this.filterExpr;
        else
            return new ExprEvalTree(null);
    }

    public ExprEvalTree getWhereExpr() {
        if (this.whereExpr != null)
            return this.whereExpr;
        else
            return new ExprEvalTree(null);
    }
}
