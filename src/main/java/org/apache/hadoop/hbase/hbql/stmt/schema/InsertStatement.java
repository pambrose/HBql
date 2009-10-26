package org.apache.hadoop.hbase.hbql.stmt.schema;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.stmt.SchemaStatement;
import org.apache.hadoop.hbase.hbql.stmt.args.InsertValues;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.select.ExprElement;
import org.apache.hadoop.hbase.hbql.stmt.util.Lists;

import java.io.IOException;
import java.util.List;

public class InsertStatement extends SchemaStatement implements PreparedStatement {

    private final List<ExprElement> columnList = Lists.newArrayList();
    private final InsertValues insertValues;

    private ConnectionImpl connection = null;
    private HRecord record = null;

    public InsertStatement(final String schemaName,
                           final List<GenericValue> columnList,
                           final InsertValues insertValues) {
        super(schemaName);

        for (final GenericValue val : columnList)
            this.columnList.add(ExprElement.newExprElement(val, null));

        this.insertValues = insertValues;
    }

    public void setConnection(final ConnectionImpl connection) throws HBqlException {
        this.connection = connection;
        this.record = SchemaManager.newHRecord(this.getSchemaName());
    }

    public void validate() throws HBqlException {

        for (final ExprElement element : this.getColumnList()) {
            element.validate(this.getSchema(), this.getConnection());
            if (!element.isSimpleColumnReference())
                throw new HBqlException(element.asString() + " is not a column reference in " + this.asString());
        }

        this.getInsertValues().validate(this);

        if (this.getColumnList().size() != this.getInsertValues().getValueCount())
            throw new HBqlException("Number of columns not equal to number of values in " + this.asString());
    }

    public int setParameter(final String name, final Object val) throws HBqlException {
        return this.getInsertValues().setParameter(name, val);
    }

    private HRecord getRecord() {
        return this.record;
    }

    public ConnectionImpl getConnection() {
        return this.connection;
    }

    private List<ExprElement> getColumnList() {
        return columnList;
    }

    private InsertValues getInsertValues() {
        return this.insertValues;
    }

    public HOutput execute() throws HBqlException, IOException {

        int cnt = 0;

        while (this.getInsertValues().hasValues()) {
            final HBatch batch = new HBatch();

            for (int i = 0; i < this.getColumnList().size(); i++) {
                this.getRecord().setCurrentValue(this.getColumnList().get(i).asString(),
                                                 this.getInsertValues().getValue(i));
            }

            batch.insert(this.getRecord());

            this.getConnection().apply(batch);
            cnt++;
        }

        return new HOutput(cnt + " record" + ((cnt > 1) ? "s" : "") + " inserted");
    }

    public void reset() {
        this.getInsertValues().reset();
        this.getRecord().reset();
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder();

        sbuf.append("INSERT INTO ");
        sbuf.append(this.getSchemaName());
        sbuf.append(" (");

        boolean firstTime = true;
        for (final ExprElement val : this.getColumnList()) {
            if (!firstTime)
                sbuf.append(", ");
            firstTime = false;

            sbuf.append(val.asString());
        }

        sbuf.append(") ");

        sbuf.append(this.getInsertValues().asString());

        return sbuf.toString();
    }
}