package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 11:07:28 PM
 */
public class QueryArgs {

    private final List<SelectColumn> selectColumnList;
    private final List<String> columnNameList = Lists.newArrayList();
    private final String tableName;
    private final WhereArgs whereExpr;
    private final HBaseSchema schema;

    public QueryArgs(final TokenStream input,
                     final List<SelectColumn> selectColumnList,
                     final String tableName,
                     final WhereArgs whereExpr,
                     final HBaseSchema schema) throws RecognitionException {
        this.tableName = tableName;
        this.selectColumnList = selectColumnList;
        this.whereExpr = whereExpr;
        this.schema = schema;

        this.validateSelectColumns(input);
    }

    private void validateSelectColumns(final TokenStream input) throws RecognitionException {

        for (final SelectColumn column : this.getSelectColumnList()) {
            final String familyName = column.getFamilyName();
            switch (column.getType()) {
                case ALLTABLECOLUMNS:
                    this.columnNameList.addAll(this.getSchema().getFamilyQualifiedNameList());
                    return;

                case ALLFAMILYCOLUMNS:
                    if (!this.getSchema().containsFamilyNameInFamilyNameMap(column.getFamilyName()))
                        throw new RecognitionException(input);

                    this.columnNameList.addAll(this.getSchema().getFieldList(column.getFamilyName()));
                    break;

                case GENERICEXPR:
                    column.setContext();
                    this.columnNameList.addAll(column.getColumnNameList());
                    break;
            }
        }
    }

    public List<SelectColumn> getSelectColumnList() {
        return this.selectColumnList;
    }

    public List<String> getColumnNameList() {
        return columnNameList;
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

    public HBaseSchema getSchema() {
        return this.schema;
    }
}
