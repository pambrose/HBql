package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.util.Sets;

import java.util.List;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 11:07:28 PM
 */
public class QueryArgs {

    private final List<SelectColumn> selectColumnList;
    private final Set<ColumnAttrib> selectColumnAttribSet = Sets.newHashSet();
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
        this.processSelectColumns();
    }

    private void processSelectColumns() throws HBqlException {

        for (final SelectColumn column : this.getSelectColumnList()) {

            column.setSchema(this.getSchema());

            switch (column.getType()) {
                case ALLTABLECOLUMNS:
                    this.selectColumnAttribSet.addAll(this.getSchema().getAllAttribs());
                    break;

                case ALLFAMILYCOLUMNS:
                    final String familyName = column.getFamilyName();
                    if (!this.getSchema().containsFamilyNameInFamilyNameMap(familyName))
                        throw new HBqlException("Invalid family name: " + familyName);

                    this.selectColumnAttribSet.addAll(this.getSchema().getAttribForFamily(familyName));
                    break;

                case GENERICEXPR:
                    this.selectColumnAttribSet.addAll(column.getFamilyQualifiedColumnNameList());
                    break;
            }
        }
    }

    public List<SelectColumn> getSelectColumnList() {
        return this.selectColumnList;
    }

    public Set<ColumnAttrib> getSelectAttribSet() {
        return this.selectColumnAttribSet;
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
