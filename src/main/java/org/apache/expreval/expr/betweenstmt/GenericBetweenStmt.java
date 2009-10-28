package org.apache.expreval.expr.betweenstmt;

import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.NotValue;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;

public abstract class GenericBetweenStmt extends NotValue<GenericBetweenStmt> implements BooleanValue {

    protected GenericBetweenStmt(final ExpressionType type,
                                 final boolean not,
                                 final GenericValue arg0,
                                 final GenericValue arg1,
                                 final GenericValue arg2) {
        super(type, not, arg0, arg1, arg2);
    }

    public String asString() {
        return this.getArg(0).asString() + notAsString() + " BETWEEN "
               + this.getArg(1).asString() + " AND " + this.getArg(2).asString();
    }
}
