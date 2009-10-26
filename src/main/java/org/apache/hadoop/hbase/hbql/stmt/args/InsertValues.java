package org.apache.hadoop.hbase.hbql.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.schema.InsertStatement;
import org.apache.hadoop.hbase.hbql.stmt.schema.SelectStatement;
import org.apache.hadoop.hbase.hbql.stmt.select.ExprElement;
import org.apache.hadoop.hbase.hbql.stmt.util.Lists;

import java.util.List;

public class InsertValues {

    private final List<ExprElement> valueList = Lists.newArrayList();
    private InsertStatement insertStatement = null;
    private boolean calledForValues = false;

    public InsertValues(final List<GenericValue> valueList) {
        for (final GenericValue val : valueList)
            this.valueList.add(ExprElement.newExprElement(val, null));
    }

    public InsertValues(final SelectStatement selectStatement) {

    }

    private List<ExprElement> getValueList() {
        return this.valueList;
    }

    public int setParameter(final String name, final Object val) throws HBqlException {

        int cnt = 0;

        for (final ExprElement expr : this.getValueList())
            cnt += expr.setParameter(name, val);

        return cnt;
    }

    private InsertStatement getInsertStatement() {
        return this.insertStatement;
    }

    public void validate(final InsertStatement insertStatement) throws HBqlException {

        this.insertStatement = insertStatement;

        for (final ExprElement element : this.getValueList()) {
            element.validate(this.getInsertStatement().getSchema(), this.getInsertStatement().getConnection());
            // Make sure values do not have column references
            if (element.hasAColumnReference())
                throw new HBqlException("Column reference " + element.asString() + " not valid in " + this.asString());
        }
    }

    public void reset() {
        this.calledForValues = false;
        for (final ExprElement expr : this.getValueList())
            expr.reset();
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder();

        sbuf.append("VALUES (");

        boolean firstTime = true;
        for (final ExprElement val : this.getValueList()) {
            if (!firstTime)
                sbuf.append(", ");
            firstTime = false;

            sbuf.append(val.asString());
        }

        sbuf.append(")");

        return sbuf.toString();
    }

    public int getValueCount() {
        return this.getValueList().size();
    }

    public Object getValue(final int i) throws HBqlException {
        return this.getValueList().get(i).evaluateConstant(0, false, true);
    }

    public boolean hasValues() {
        this.calledForValues = !this.calledForValues;

        return this.calledForValues;
    }
}
