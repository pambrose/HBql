package org.apache.hadoop.hbase.contrib.hbql.statement.args;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.contrib.hbql.statement.InsertStatement;

import java.io.IOException;
import java.util.List;

public abstract class InsertValueSource {

    private InsertStatement insertStatement = null;

    public void setInsertStatement(InsertStatement insertStatement) {
        this.insertStatement = insertStatement;
    }

    protected InsertStatement getInsertStatement() {
        return this.insertStatement;
    }

    public abstract int setParameter(String name, Object val) throws HBqlException;

    public abstract void validate() throws HBqlException;

    public abstract void reset();

    public abstract String asString();

    public abstract Object getValue(int i) throws HBqlException;

    public abstract boolean isDefaultValue(int i) throws HBqlException;

    public abstract boolean hasValues();

    public abstract void execute() throws HBqlException, IOException;

    public abstract List<Class<? extends GenericValue>> getValuesTypeList() throws HBqlException;
}
