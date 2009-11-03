package org.apache.hadoop.hbase.contrib.hbql.statement.args;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.contrib.hbql.schema.HBaseSchema;
import org.apache.hadoop.hbase.contrib.hbql.statement.select.SingleExpressionContext;

import java.util.List;

public class InsertSingleRow extends InsertValueSource {

    private final List<SingleExpressionContext> valueList = Lists.newArrayList();
    private boolean calledForValues = false;

    public InsertSingleRow(final List<GenericValue> valueList) {
        for (final GenericValue val : valueList)
            this.getValueList().add(SingleExpressionContext.newSingleExpression(val, null));
    }

    private List<SingleExpressionContext> getValueList() {
        return this.valueList;
    }

    public int setParameter(final String name, final Object val) throws HBqlException {

        int cnt = 0;

        for (final SingleExpressionContext expr : this.getValueList())
            cnt += expr.setParameter(name, val);

        return cnt;
    }

    public void validate() throws HBqlException {

        final HBaseSchema schema = this.getInsertStatement().getSchema();
        final ConnectionImpl conn = this.getInsertStatement().getConnection();

        for (final SingleExpressionContext element : this.getValueList()) {
            element.validate(schema, conn);

            // Make sure values do not have column references
            if (element.hasAColumnReference())
                throw new HBqlException("Column reference " + element.asString() + " not valid in " + this.asString());
        }
    }

    public void execute() {
        // No op
    }

    public void reset() {
        this.calledForValues = false;
        for (final SingleExpressionContext expr : this.getValueList())
            expr.reset();
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder();

        sbuf.append("VALUES (");

        boolean firstTime = true;
        for (final SingleExpressionContext val : this.getValueList()) {
            if (!firstTime)
                sbuf.append(", ");
            firstTime = false;

            sbuf.append(val.asString());
        }

        sbuf.append(")");

        return sbuf.toString();
    }

    public boolean isDefaultValue(final int i) throws HBqlException {
        return this.getValueList().get(i).isDefaultKeyword();
    }

    public Object getValue(final int i) throws HBqlException {
        return this.getValueList().get(i).evaluateConstant(0, false, null);
    }

    public List<Class<? extends GenericValue>> getValuesTypeList() throws HBqlException {
        final List<Class<? extends GenericValue>> typeList = Lists.newArrayList();
        for (final SingleExpressionContext element : this.getValueList()) {
            final Class<? extends GenericValue> type = element.getExpressionType();
            typeList.add(type);
        }
        return typeList;
    }

    public boolean hasValues() {
        this.calledForValues = !this.calledForValues;
        return this.calledForValues;
    }
}
