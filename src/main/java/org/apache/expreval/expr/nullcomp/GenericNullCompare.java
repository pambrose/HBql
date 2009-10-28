package org.apache.expreval.expr.nullcomp;

import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.NotValue;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;

public abstract class GenericNullCompare extends NotValue<GenericNullCompare> implements BooleanValue {

    protected GenericNullCompare(final ExpressionType type, final boolean not, final GenericValue arg0) {
        super(type, not, arg0);
    }

    public String asString() {
        return this.getArg(0).asString() + " IS" + notAsString() + " NULL";
    }
}