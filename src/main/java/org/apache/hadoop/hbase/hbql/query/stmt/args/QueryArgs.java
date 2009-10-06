package org.apache.hadoop.hbase.hbql.query.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.stmt.select.SelectElement;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
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

    private final List<SelectElement> selectElementList;
    private final List<ColumnAttrib> selectColumnAttribList = Lists.newArrayList();
    private final String tableName;
    private final WhereArgs whereArgs;

    private HBaseSchema schema = null;

    public QueryArgs(final List<SelectElement> selectElementList,
                     final String tableName,
                     final WhereArgs whereArgs) {
        this.tableName = tableName;
        this.selectElementList = selectElementList;
        this.whereArgs = whereArgs != null ? whereArgs : new WhereArgs();
    }

    public void validate(final HConnection connection) throws HBqlException {

        this.schema = HBaseSchema.findSchema(this.getTableName());

        for (final SelectElement selectElement : this.getSelectElementList())
            selectElement.validate(connection, this.getSchema(), this.getSelectAttribList());

        // Make sure there are no duplicate aliases in list
        this.checkForDuplicateAsNames();

        if (this.getWhereArgs().getServerExprTree() != null)
            this.getWhereArgs().getServerExprTree().setUseHBaseResult(false);

        if (this.getWhereArgs().getClientExprTree() != null)
            this.getWhereArgs().getClientExprTree().setUseHBaseResult(true);
    }

    private void checkForDuplicateAsNames() throws HBqlException {
        final Set<String> asNameSet = Sets.newHashSet();
        for (final SelectElement selectElement : this.getSelectElementList()) {
            final String asName = selectElement.getAsName();
            if (asName == null)
                continue;

            if (asNameSet.contains(asName))
                throw new HBqlException("Duplicate select name " + asName + " in select list");

            asNameSet.add(asName);
        }
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
        return this.whereArgs;
    }

    public HBaseSchema getSchema() {
        return this.schema;
    }

    public void setParameter(final String name, final Object val) throws HBqlException {
        for (final SelectElement selectElement : this.getSelectElementList())
            selectElement.setParameter(name, val);

        this.getWhereArgs().setParameter(name, val);
    }
}
