package org.apache.hadoop.hbase.hbql.stmt.expr.literal;

import org.apache.hadoop.hbase.hbql.stmt.expr.node.DoubleValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class DoubleLiteral extends GenericLiteral<Double> implements DoubleValue {

    public DoubleLiteral(final Double value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return DoubleValue.class;
    }
}