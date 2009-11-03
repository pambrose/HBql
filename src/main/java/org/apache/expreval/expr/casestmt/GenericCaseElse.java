package org.apache.expreval.expr.casestmt;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.DelegateStmt;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

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