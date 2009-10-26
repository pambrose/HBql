package org.apache.hadoop.hbase.hbql.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.stmt.schema.InsertStatement;
import org.apache.hadoop.hbase.hbql.stmt.schema.SelectStatement;

public class InsertSelectValues extends InsertValueSource {

    private final SelectStatement selectStatement;
    private boolean calledForValues = false;

    public InsertSelectValues(final SelectStatement selectStatement) {
        this.selectStatement = selectStatement;
    }

    public SelectStatement getSelectStatement() {
        return this.selectStatement;
    }

    public int setParameter(final String name, final Object val) throws HBqlException {
        return this.getSelectStatement().setParameter(name, val);
    }

    public void validate(final InsertStatement insertStatement) throws HBqlException {
        super.validate(insertStatement);

        this.getSelectStatement().validate(this.getInsertStatement().getConnection());
    }

    public void reset() {
        this.calledForValues = false;
        this.getSelectStatement().reset();
    }

    public String asString() {
        return this.getSelectStatement().asString();
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