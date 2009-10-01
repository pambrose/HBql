package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
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
    private final List<ColumnAttrib> selectColumnAttribList = Lists.newArrayList();
    private final String tableName;
    private final WhereArgs whereArgs;

    private HBaseSchema schema = null;

    public QueryArgs(final List<SelectColumn> selectColumnList,
                     final String tableName,
                     final WhereArgs whereArgs) throws RecognitionException {
        this.tableName = tableName;
        this.selectColumnList = selectColumnList;
        this.whereArgs = whereArgs;
    }

    public void validate() throws HBqlException {
        this.schema = HBaseSchema.findSchema(this.getTableName());
        this.processSelectColumns();

        if (this.getWhereArgs().getServerExprTree() != null)
            this.getWhereArgs().getServerExprTree().setUseHBaseResult(true);

        if (this.getWhereArgs().getClientExprTree() != null)
            this.getWhereArgs().getClientExprTree().setUseHBaseResult(true);
    }

    private void processSelectColumns() throws HBqlException {

        for (final SelectColumn selectColumn : this.getSelectColumnList()) {

            selectColumn.setSchema(this.getSchema());

            switch (selectColumn.getType()) {
                case ALLTABLECOLUMNS:
                    //for (final )
                    this.selectColumnAttribList.addAll(this.getSchema().getAllAttribs());

                    break;

                case ALLFAMILYCOLUMNS:
                    final String familyName = selectColumn.getFamilyName();
                    if (!this.getSchema().containsFamilyNameInFamilyNameMap(familyName))
                        throw new HBqlException("Invalid family name: " + familyName);

                    this.selectColumnAttribList.addAll(this.getSchema().getAttribForFamily(familyName));
                    break;

                case GENERICEXPR:
                    this.selectColumnAttribList.addAll(selectColumn.getFamilyQualifiedColumnNameList());
                    break;
            }
        }
    }

    public List<SelectColumn> getSelectColumnList() {
        return this.selectColumnList;
    }

    public List<ColumnAttrib> getSelectAttribList() {
        return this.selectColumnAttribList;
    }

    public String getTableName() {
        return this.tableName;
    }

    public WhereArgs getWhereArgs() {
        if (this.whereArgs != null)
            return this.whereArgs;
        else
            return new WhereArgs();
    }

    public HBaseSchema getSchema() {
        return this.schema;
    }
}
