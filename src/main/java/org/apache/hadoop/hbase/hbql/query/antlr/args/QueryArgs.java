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

    private final List<SelectElement> selectElementList;
    private final List<ColumnAttrib> selectColumnAttribList = Lists.newArrayList();
    private final String tableName;
    private final WhereArgs whereArgs;

    private HBaseSchema schema = null;

    public QueryArgs(final List<SelectElement> selectElementList,
                     final String tableName,
                     final WhereArgs whereArgs) throws RecognitionException {
        this.tableName = tableName;
        this.selectElementList = selectElementList;
        this.whereArgs = whereArgs;
    }

    public void validate() throws HBqlException {
        this.schema = HBaseSchema.findSchema(this.getTableName());

        for (final SelectElement selectElement : this.getSelectElementList())
            selectElement.validate(this.getSchema(), this.getSelectAttribList());

        if (this.getWhereArgs().getServerExprTree() != null)
            this.getWhereArgs().getServerExprTree().setUseHBaseResult(true);

        if (this.getWhereArgs().getClientExprTree() != null)
            this.getWhereArgs().getClientExprTree().setUseHBaseResult(true);
    }

    public List<SelectElement> getSelectElementList() {
        return this.selectElementList;
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
