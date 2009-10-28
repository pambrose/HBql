package org.apache.hadoop.hbase.hbql.stmt.schema;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.stmt.SchemaStatement;
import org.apache.hadoop.hbase.hbql.stmt.args.InsertValueSource;
import org.apache.hadoop.hbase.hbql.stmt.expr.literal.DefaultKeyword;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.select.SingleExpression;
import org.apache.hadoop.hbase.hbql.stmt.util.HUtil;
import org.apache.hadoop.hbase.hbql.stmt.util.Lists;

import java.io.IOException;
import java.util.List;

public class InsertStatement extends SchemaStatement implements PreparedStatement {

    private final List<SingleExpression> columnList = Lists.newArrayList();
    private final InsertValueSource valueSource;

    private ConnectionImpl connection = null;
    private HRecord record = null;
    private boolean validated = false;

    public InsertStatement(final String schemaName,
                           final List<GenericValue> columnList,
                           final InsertValueSource valueSource) {
        super(schemaName);

        for (final GenericValue val : columnList)
            this.getColumnList().add(SingleExpression.newSingleExpression(val, null));

        this.valueSource = valueSource;
        this.getValueSource().setInsertStatement(this);
    }

    public void validate(final ConnectionImpl conn) throws HBqlException {

        if (validated)
            return;

        this.validated = true;

        this.connection = conn;
        this.record = SchemaManager.newHRecord(this.getSchemaName());

        for (final SingleExpression element : this.getColumnList()) {

            element.validate(this.getSchema(), this.getConnection());

            if (!element.isASimpleColumnReference())
                throw new TypeException(element.asString() + " is not a column reference in " + this.asString());
        }

        if (!this.hasAKeyValue())
            throw new TypeException("Missing a key value in attribute list in " + this.asString());

        this.getValueSource().validate();
    }

    public void validateTypes() throws HBqlException {

        final List<Class<? extends GenericValue>> columnsTypeList = this.getColumnsTypeList();
        final List<Class<? extends GenericValue>> valuesTypeList = this.getValueSource().getValuesTypeList();

        if (columnsTypeList.size() != valuesTypeList.size())
            throw new HBqlException("Number of columns not equal to number of values in " + this.asString());

        for (int i = 0; i < columnsTypeList.size(); i++) {

            final Class<? extends GenericValue> type1 = columnsTypeList.get(i);
            final Class<? extends GenericValue> type2 = valuesTypeList.get(i);

            // Skip Default values
            if (type2 == DefaultKeyword.class) {
                final String name = this.getColumnList().get(i).asString();
                final ColumnAttrib attrib = this.getSchema().getAttribByVariableName(name);
                if (!attrib.hasDefaultArg())
                    throw new HBqlException("No DEFAULT value specified for " + attrib.getNameToUseInExceptions()
                                            + " in " + this.asString());
                continue;
            }

            if (!HUtil.isParentClass(type1, type2))
                throw new TypeException("Type mismatch in argument " + i
                                        + " expecting " + type1.getSimpleName()
                                        + " but found " + type2.getSimpleName()
                                        + " in " + this.asString());
        }
    }

    private List<Class<? extends GenericValue>> getColumnsTypeList() throws HBqlException {
        final List<Class<? extends GenericValue>> typeList = Lists.newArrayList();
        for (final SingleExpression element : this.getColumnList()) {
            final Class<? extends GenericValue> type = element.getExpressionType();
            typeList.add(type);
        }
        return typeList;
    }

    private boolean hasAKeyValue() {
        for (final SingleExpression element : this.getColumnList()) {
            if (element.isAKeyValue())
                return true;
        }
        return false;
    }

    public int setParameter(final String name, final Object val) throws HBqlException {
        final int cnt = this.getValueSource().setParameter(name, val);
        if (cnt == 0)
            throw new HBqlException("Parameter name " + name + " does not exist in " + this.asString());
        return cnt;
    }

    private HRecord getRecord() {
        return this.record;
    }

    public ConnectionImpl getConnection() {
        return this.connection;
    }

    private List<SingleExpression> getColumnList() {
        return columnList;
    }

    private InsertValueSource getValueSource() {
        return this.valueSource;
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        this.validate(conn);

        this.validateTypes();

        int cnt = 0;

        this.getValueSource().execute();

        while (this.getValueSource().hasValues()) {

            final HBatch batch = new HBatch();

            for (int i = 0; i < this.getColumnList().size(); i++) {
                final String name = this.getColumnList().get(i).asString();
                final Object val;
                if (this.getValueSource().isDefaultValue(i)) {
                    final ColumnAttrib attrib = this.getSchema().getAttribByVariableName(name);
                    val = attrib.getDefaultValue();
                }
                else {
                    val = this.getValueSource().getValue(i);
                }
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
        for (final SingleExpression val : this.getColumnList()) {
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