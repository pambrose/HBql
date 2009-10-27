package org.apache.hadoop.hbase.hbql.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.schema.InsertStatement;

import java.io.IOException;
import java.util.List;

public abstract class InsertValueSource {

    private InsertStatement insertStatement = null;

    public abstract int setParameter(String name, Object val) throws HBqlException;

    public abstract void validate() throws HBqlException;

    public void setInsertStatement(InsertStatement insertStatement) {
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

    public abstract void execute() throws HBqlException, IOException;

    public abstract List<Class<? extends GenericValue>> getValuesTypeList() throws HBqlException;
}
