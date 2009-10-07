package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.FloatValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class FloatLiteral extends GenericLiteral implements FloatValue {

    private final Float value;

    public FloatLiteral(final Float value) {
        this.value = value;
    }

    public Float getValue(final Object object) {
        return this.value;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return FloatValue.class;
    }

    public String asString() {
        return "" + this.value;
    }
}