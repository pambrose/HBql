package org.apache.hadoop.hbase.hbql.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.stmt.schema.InsertStatement;

public abstract class InsertValueSource {

    private InsertStatement insertStatement = null;

    public abstract int setParameter(String name, Object val) throws HBqlException;

    public void validate(InsertStatement insertStatement) throws HBqlException {
        this.insertStatement = insertStatement;
    }

    public abstract void reset();

    public abstract String asString();

    public abstract int getValueCount();

    public abstract Object getValue(int i) throws HBqlException;

    public abstract boolean hasValues();

    protected InsertStatement getInsertStatement() {
        return this.insertStatement;
    }
}
