package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;
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
    private final List<VariableAttrib> selectVariableAttribList = Lists.newArrayList();
    private final String tableName;
    private final WhereArgs whereExpr;

    private HBaseSchema schema = null;

    public QueryArgs(final List<SelectColumn> selectColumnList,
                     final String tableName,
                     final WhereArgs whereExpr) throws RecognitionException {
        this.tableName = tableName;
        this.selectColumnList = selectColumnList;
        this.whereExpr = whereExpr;
    }

    public void validate() throws HBqlException {
        this.schema = HBaseSchema.findSchema(this.getTableName());
        this.validateSelectColumns();
    }

    private void validateSelectColumns() throws HBqlException {

        for (final SelectColumn column : this.getSelectColumnList()) {

            switch (column.getType()) {
                case ALLTABLECOLUMNS:
                    this.selectVariableAttribList.addAll(this.getSchema().getAllVariableAttrib());
                    return;

                case ALLFAMILYCOLUMNS:
                    if (!this.getSchema().containsFamilyNameInFamilyNameMap(column.getFamilyName()))
                        throw new HBqlException("Invalid family name: " + column.getFamilyName());

                    this.selectVariableAttribList
                            .addAll(this.getSchema().getVariableAttribForFamily(column.getFamilyName()));
                    break;

                case GENERICEXPR:
                    column.setSchema(schema);
                    this.selectVariableAttribList.addAll(column.getFamilyQualifiedColumnNameList());
                    break;
            }
        }
    }

    public List<SelectColumn> getSelectColumnList() {
        return this.selectColumnList;
    }

    public List<VariableAttrib> getSelectVariableAttribList() {
        return this.selectVariableAttribList;
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
