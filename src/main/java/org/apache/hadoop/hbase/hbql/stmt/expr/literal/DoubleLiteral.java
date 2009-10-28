package org.apache.hadoop.hbase.hbql.stmt.expr.literal;

import org.apache.hadoop.hbase.hbql.stmt.expr.node.DoubleValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class DoubleLiteral extends GenericLiteral<Double> implements DoubleValue {

    public static GenericValue valueOf(final String value) {
        final String upper = value.toUpperCase();
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