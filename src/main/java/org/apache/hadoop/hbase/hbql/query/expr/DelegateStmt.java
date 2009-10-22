package org.apache.hadoop.hbase.hbql.query.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

import java.util.List;

public abstract class DelegateStmt<T extends GenericExpr> extends GenericExpr {

    private T typedExpr = null;

    protected DelegateStmt(final GenericExpr.Type type, GenericValue... args) {
        super(type, args);
    }

    protected DelegateStmt(final Type type, final List<GenericValue> args) {
        super(type, args);
    }

    protected DelegateStmt(final Type type, final GenericValue arg, final List<GenericValue> argList) {
        super(type, arg, argList);
    }

    protected T getTypedExpr() {
        return typedExpr;
    }

    protected void setTypedExpr(final T typedExpr) throws HBqlException {
        this.typedExpr = typedExpr;

        this.getTypedExpr().setExprContext(this.getExprContext());
    }
}
