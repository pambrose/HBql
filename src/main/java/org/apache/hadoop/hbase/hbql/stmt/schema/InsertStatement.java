package org.apache.hadoop.hbase.hbql.stmt.schema;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.stmt.SchemaStatement;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.select.ExprElement;
import org.apache.hadoop.hbase.hbql.stmt.util.Lists;

import java.io.IOException;
import java.util.List;

public class InsertStatement extends SchemaStatement implements PreparedStatement {

    private final List<ExprElement> columnList = Lists.newArrayList();
    private final List<ExprElement> valueList = Lists.newArrayList();

    private ConnectionImpl connection = null;
    private HRecord record = null;

    public InsertStatement(final String schemaName,
                           final List<GenericValue> columnList,
                           final List<GenericValue> valueList) {
        super(schemaName);

        for (final GenericValue val : columnList)
            this.columnList.add(ExprElement.newExprElement(val, null));

        for (final GenericValue val : valueList)
            this.valueList.add(ExprElement.newExprElement(val, null));
    }

    public void setConnection(final ConnectionImpl connection) throws HBqlException {
        this.connection = connection;
        this.record = SchemaManager.newHRecord(this.getSchemaName());
    }

    public void validate() throws HBqlException {

        for (final ExprElement val : this.getColumnList()) {
            val.validate(this.getSchema(), this.getConnection());
            if (!val.isSimpleColumnReference())
                throw new HBqlException(val.asString() + " is not a column reference in " + this.asString());
        }

        for (final ExprElement val : this.getValueList()) {
            val.validate(this.getSchema(), this.getConnection());
            if (val.hasAColumnReference())
                throw new HBqlException("Column reference " + val.asString() + " now valid in " + this.asString());
        }

        if (this.getColumnList().size() != this.getValueList().size())
            throw new HBqlException("Number of columns not equal to number of values in " + this.asString());

        // Make sure values do not have column references

    }

    public int setParameter(final String name, final Object val) throws HBqlException {

        int cnt = 0;

        for (final ExprElement expr : this.getValueList())
            cnt += expr.setParameter(name, val);

        return cnt;
    }

    private HRecord getRecord() {
        return this.record;
    }

    private ConnectionImpl getConnection() {
        return this.connection;
    }

    private List<ExprElement> getColumnList() {
        return columnList;
    }

    private List<ExprElement> getValueList() {
        return valueList;
    }

    public HOutput execute() throws HBqlException, IOException {

        final HBatch batch = new HBatch();

        for (int i = 0; i < this.getColumnList().size(); i++) {
            this.getRecord().setCurrentValue(this.getColumnList().get(i).asString(),
                                             this.getValueList().get(i).evaluateConstant(0, false, true));
        }

        batch.insert(this.getRecord());

        this.getConnection().apply(batch);

        return new HOutput("Record inserted");
    }

    public void reset() {

        for (final ExprElement expr : this.getValueList())
            expr.reset();

        this.getRecord().reset();
    }

    public String asString() {
        final StringBuilder sbuf = new StringBuilder();

        sbuf.append("INSERT INTO " + this.getSchemaName() + " (");

        boolean firstTime = true;
        for (final ExprElement val : this.getColumnList()) {
            if (!firstTime)
                sbuf.append(", ");
            firstTime = false;

            sbuf.append(val.asString());
        }

        sbuf.append(") VALUES (");

        firstTime = true;
        for (final ExprElement val : this.getValueList()) {
            if (!firstTime)
                sbuf.append(", ");
            firstTime = false;

            sbuf.append(val.asString());
        }

        sbuf.append(")");

        return sbuf.toString();
    }
}