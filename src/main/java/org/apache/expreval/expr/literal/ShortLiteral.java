package org.apache.expreval.expr.literal;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.ShortValue;

public class ShortLiteral extends GenericLiteral<Short> implements ShortValue {

    public ShortLiteral(final Short value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return ShortValue.class;
    }
}