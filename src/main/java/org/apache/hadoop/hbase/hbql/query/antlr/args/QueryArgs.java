package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.query.schema.ExprSchema;

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
    private final ExprSchema schema;

    public QueryArgs(final List<String> columnList,
                     final String tableName,
                     final WhereArgs whereExpr,
                     final ExprSchema schema) {
        this.tableName = tableName;
        this.columnList = columnList;
        this.whereExpr = whereExpr;
        this.schema = schema;
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

    public ExprSchema getSchema() {
        return this.schema;
    }
}
