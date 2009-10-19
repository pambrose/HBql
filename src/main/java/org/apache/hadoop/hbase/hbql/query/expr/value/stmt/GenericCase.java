package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.GenericExpr;

import java.util.List;

public abstract class GenericCase extends GenericExpr {

    private final List<GenericCaseWhen> whenExprList;
    private GenericCaseElse elseExpr;

    protected GenericCase(final Type type, final List<GenericCaseWhen> whenExprList, final GenericCaseElse elseExpr) {
        super(type);
        this.whenExprList = whenExprList;
        this.elseExpr = elseExpr;
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if ((Boolean)this.getArg(0).getValue(object))
            return this.getArg(1).getValue(object);
        else
            return this.getArg(2).getValue(object);
    }

    public String asString() {
        return "IF " + this.getArg(0).asString() + " THEN "
               + this.getArg(1).asString()
               + " ELSE " + this.getArg(2).asString() + " END";
    }

    protected List<GenericCaseWhen> getWhenExprList() {
        return this.whenExprList;
    }

    protected GenericCaseElse getElseExpr() {
        return this.elseExpr;
    }

    protected void setElseExpr(final GenericCaseElse elseExpr) {
        this.elseExpr = elseExpr;
    }
}