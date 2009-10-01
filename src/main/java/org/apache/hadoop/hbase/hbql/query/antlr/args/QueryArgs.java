package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.DelegateColumn;
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

    private final List<SelectColumn> originalSelectColumnList;
    private final List<SelectColumn> derivedSelectColumnList = Lists.newArrayList();
    private final List<ColumnAttrib> selectColumnAttribList = Lists.newArrayList();
    private final String tableName;
    private final WhereArgs whereArgs;

    private HBaseSchema schema = null;

    public QueryArgs(final List<SelectColumn> originalSelectColumnList,
                     final String tableName,
                     final WhereArgs whereArgs) throws RecognitionException {
        this.tableName = tableName;
        this.originalSelectColumnList = originalSelectColumnList;
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

        for (final SelectColumn selectColumn : this.originalSelectColumnList) {

            selectColumn.setSchema(this.getSchema());

            switch (selectColumn.getType()) {
                case ALLTABLECOLUMNS:
                    for (final ColumnAttrib attrib : this.getSchema().getAllAttribs())
                        this.getSelectColumnList().add(this.getSelectColumnForColumnAttrib(attrib));
                    this.selectColumnAttribList.addAll(this.getSchema().getAllAttribs());

                    break;

                case ALLFAMILYCOLUMNS:
                    final String familyName = selectColumn.getFamilyName();
                    if (!this.getSchema().containsFamilyNameInFamilyNameMap(familyName))
                        throw new HBqlException("Invalid family name: " + familyName);

                    for (final ColumnAttrib attrib : this.getSchema().getAttribForFamily(familyName))
                        this.getSelectColumnList().add(this.getSelectColumnForColumnAttrib(attrib));
                    this.selectColumnAttribList.addAll(this.getSchema().getAttribForFamily(familyName));
                    break;

                case GENERICEXPR:
                    this.getSelectColumnList().add(selectColumn);
                    this.selectColumnAttribList.addAll(selectColumn.getFamilyQualifiedColumnNameList());
                    break;
            }
        }
    }

    private SelectColumn getSelectColumnForColumnAttrib(final ColumnAttrib attrib) {
        final GenericValue val = new DelegateColumn(attrib.getAliasName());
        return SelectColumn.newColumn(val, attrib.getAliasName());
    }

    public List<SelectColumn> getSelectColumnList() {
        return this.derivedSelectColumnList;
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
