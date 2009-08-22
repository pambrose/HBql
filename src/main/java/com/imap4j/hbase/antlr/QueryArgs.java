package com.imap4j.hbase.antlr;

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

    public QueryArgs(final List<String> columnList, final String tableName) {
        this.tableName = tableName;
        this.columnList = columnList;
    }

    public List<String> getColumnList() {
        return columnList;
    }

    public String getTableName() {
        return tableName;
    }
}
