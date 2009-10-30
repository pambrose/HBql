package org.apache.expreval.expr.literal;

import org.apache.expreval.expr.node.DoubleValue;
import org.apache.expreval.expr.node.GenericValue;

public class DoubleLiteral extends GenericLiteral<Double> implements DoubleValue {

    public static GenericValue valueOf(final String value) {
        final String upper = (value != null) ? value.toUpperCase() : "0";
        return upper.endsWith("F") ? new FloatLiteral(upper) : new DoubleLiteral(upper);
    }

    public DoubleLiteral(final String value) {
        super(Double.valueOf(value.endsWith("D") ? value.substring(0, value.length() - 1) : value));
    }

    public DoubleLiteral(final Double value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return DoubleValue.class;
    }
}