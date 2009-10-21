package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public abstract class GenericCaseWhen extends DelegateStmt<GenericCaseWhen> {

    protected GenericCaseWhen(final Type type,
                              final GenericValue arg0,
                              final GenericValue arg1) {
        super(type, arg0, arg1);
    }

    public boolean getPredicateValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)this.getArg(0).getValue(object);
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getArg(1).getValue(object);
    }

    public String asString() {
        return "WHEN " + this.getArg(0).asString() + " THEN " + this.getArg(1).asString() + " ";
    }
}