package org.apache.expreval.expr;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.expr.node.GenericValue;

import java.util.List;

public abstract class DelegateStmt<T extends GenericExpression> extends GenericExpression {

    private T typedExpr = null;

    protected DelegateStmt(final ExpressionType type, GenericValue... args) {
        super(type, args);
    }

    protected DelegateStmt(final ExpressionType type, final List<GenericValue> args) {
        super(type, args);
    }

    protected DelegateStmt(final ExpressionType type, final GenericValue arg, final List<GenericValue> argList) {
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
