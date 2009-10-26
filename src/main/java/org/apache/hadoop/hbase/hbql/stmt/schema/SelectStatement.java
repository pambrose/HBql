package org.apache.hadoop.hbase.hbql.stmt.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.stmt.SchemaStatement;
import org.apache.hadoop.hbase.hbql.stmt.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.stmt.select.SelectElement;
import org.apache.hadoop.hbase.hbql.stmt.util.Lists;
import org.apache.hadoop.hbase.hbql.stmt.util.Sets;

import java.util.List;
import java.util.Set;

public class SelectStatement extends SchemaStatement {

    private final List<SelectElement> selectElementList;
    private final List<ColumnAttrib> selectColumnAttribList = Lists.newArrayList();
    private final WhereArgs whereArgs;

    private int expressionCounter = 0;

    public SelectStatement(final List<SelectElement> selectElementList,
                           final String schemaName,
                           final WhereArgs whereArgs) {
        super(schemaName);
        this.selectElementList = selectElementList;
        this.whereArgs = whereArgs != null ? whereArgs : new WhereArgs();
    }

    public String getNextExpressionName() {
        return ":expr-" + this.expressionCounter++;
    }

    public void validate(final HConnection connection) throws HBqlException {

        this.getSelectAttribList().clear();

        for (final SelectElement element : this.getSelectElementList()) {
            element.validate(this.getSchema(), connection);
            element.assignAsNamesForExpressions(this);
            this.getSelectAttribList().addAll(element.getAttribsUsedInExpr());
        }

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

            if (selectElement.hasAsName()) {
                final String asName = selectElement.getAsName();

                if (asNameSet.contains(asName))
                    throw new HBqlException("Duplicate select name " + asName + " in select list");

                asNameSet.add(asName);
            }
        }
    }

    public boolean hasAsName(final String name) {

        for (final SelectElement selectElement : this.getSelectElementList())
            if (selectElement.hasAsName() && selectElement.getAsName().equals(name))
                return true;

        return false;
    }

    public List<SelectElement> getSelectElementList() {
        return this.selectElementList;
    }

    public List<ColumnAttrib> getSelectAttribList() {
        return this.selectColumnAttribList;
    }

    public WhereArgs getWhereArgs() {
        return this.whereArgs;
    }

    public int setParameter(final String name, final Object val) throws HBqlException {

        int cnt = 0;

        for (final SelectElement selectElement : this.getSelectElementList())
            cnt += selectElement.setParameter(name, val);

        cnt += this.getWhereArgs().setParameter(name, val);

        return cnt;
    }
}
