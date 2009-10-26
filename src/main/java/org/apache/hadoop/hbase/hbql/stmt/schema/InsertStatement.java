package org.apache.hadoop.hbase.hbql.stmt.schema;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.stmt.SchemaStatement;
import org.apache.hadoop.hbase.hbql.stmt.args.InsertValueSource;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.select.ExprElement;
import org.apache.hadoop.hbase.hbql.stmt.util.Lists;

import java.io.IOException;
import java.util.List;

public class InsertStatement extends SchemaStatement implements PreparedStatement {

    private final List<ExprElement> columnList = Lists.newArrayList();
    private final InsertValueSource valueSource;

    private ConnectionImpl connection = null;
    private HRecord record = null;
    private boolean validated = false;

    public InsertStatement(final String schemaName,
                           final List<GenericValue> columnList,
                           final InsertValueSource valueSource) {
        super(schemaName);

        for (final GenericValue val : columnList)
            this.columnList.add(ExprElement.newExprElement(val, null));

        this.valueSource = valueSource;
    }

    public void validate(final ConnectionImpl conn) throws HBqlException {

        if (validated)
            return;

        this.connection = conn;
        this.record = SchemaManager.newHRecord(this.getSchemaName());

        this.validated = true;

        for (final ExprElement element : this.getColumnList()) {

            element.validate(this.getSchema(), this.getConnection());

            if (!element.isASimpleColumnReference())
                throw new HBqlException(element.asString() + " is not a column reference in " + this.asString());
        }

        if (!this.hasAKeyValue())
            throw new HBqlException("Missing a key value in attribute list in " + this.asString());

        this.getValueSource().validate(this);

        if (this.getColumnList().size() != this.getValueSource().getValueCount())
            throw new HBqlException("Number of columns not equal to number of values in " + this.asString());

        // TODO do type matching on attribs and values
        for (final ExprElement element : this.getColumnList()) {
            final ColumnAttrib attrib = element.getColumnAttrib();
        }
    }

    private boolean hasAKeyValue() {
        for (final ExprElement element : this.getColumnList()) {
            if (!element.isAKeyValue())
                return true;
        }
        return false;
    }

    public int setParameter(final String name, final Object val) throws HBqlException {
        return this.getValueSource().setParameter(name, val);
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

    private InsertValueSource getValueSource() {
        return this.valueSource;
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        this.validate(conn);

        int cnt = 0;

        this.getValueSource().execute();

        while (this.getValueSource().hasValues()) {
            final HBatch batch = new HBatch();

            for (int i = 0; i < this.getColumnList().size(); i++) {
                final String name = this.getColumnList().get(i).asString();
                final Object val = this.getValueSource().getValue(i);
                this.getRecord().setCurrentValue(name, val);
            }

            batch.insert(this.getRecord());

            conn.apply(batch);
            cnt++;
        }

        return new HOutput(cnt + " record" + ((cnt > 1) ? "s" : "") + " inserted");
    }

    public HOutput execute() throws HBqlException, IOException {
        return this.execute(this.getConnection());
    }

    public void reset() {
        this.getValueSource().reset();
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
        sbuf.append(this.getValueSource().asString());
        return sbuf.toString();
    }
}