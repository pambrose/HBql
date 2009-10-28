package org.apache.hadoop.hbase.hbql.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.stmt.select.SingleExpression;
import org.apache.hadoop.hbase.hbql.stmt.util.Lists;

import java.util.List;

public class InsertSingleRow extends InsertValueSource {

    private final List<SingleExpression> valueList = Lists.newArrayList();
    private boolean calledForValues = false;

    public InsertSingleRow(final List<GenericValue> valueList) {
        for (final GenericValue val : valueList)
            this.getValueList().add(SingleExpression.newSingleExpression(val, null));
    }

    private List<SingleExpression> getValueList() {
        return this.valueList;
    }

    public int setParameter(final String name, final Object val) throws HBqlException {

        int cnt = 0;

        for (final SingleExpression expr : this.getValueList())
            cnt += expr.setParameter(name, val);

        return cnt;
    }

    public void validate() throws HBqlException {

        final HBaseSchema schema = this.getInsertStatement().getSchema();
        final ConnectionImpl conn = this.getInsertStatement().getConnection();

        for (final SingleExpression element : this.getValueList()) {
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
        for (final SingleExpression expr : this.getValueList())
            expr.reset();
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder();

        sbuf.append("VALUES (");

        boolean firstTime = true;
        for (final SingleExpression val : this.getValueList()) {
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
        for (final SingleExpression element : this.getValueList()) {
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
