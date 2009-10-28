package org.apache.hadoop.hbase.hbql.stmt.expr.casestmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.DelegateStmt;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExpressionType;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public abstract class GenericCaseElse extends DelegateStmt<GenericCaseElse> {

    protected GenericCaseElse(final ExpressionType type, final GenericValue arg0) {
        super(type, arg0);
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getArg(0).getValue(object);
    }

    public String asString() {
        return "ELSE " + this.getArg(0).asString() + " ";
    }
}