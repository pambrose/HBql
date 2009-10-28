package org.apache.expreval.expr.literal;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.IntegerValue;

public class IntegerLiteral extends GenericLiteral<Integer> implements IntegerValue {

    public IntegerLiteral(final String value) {
        super(Integer.valueOf(value));
    }

    public IntegerLiteral(final Integer value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return IntegerValue.class;
    }
}