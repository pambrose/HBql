package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DoubleValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class DoubleLiteral extends GenericLiteral implements DoubleValue {

    private final Double value;

    public DoubleLiteral(final Double value) {
        this.value = value;
    }

    public Double getValue(final Object object) {
        return this.value;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return DoubleValue.class;
    }

    public String asString() {
        return "" + this.value;
    }
}