package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ShortValue;

public class ShortLiteral extends GenericLiteral implements ShortValue {

    private final Short value;

    public ShortLiteral(final Short value) {
        this.value = value;
    }

    public Short getValue(final Object object) {
        return this.value;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return ShortValue.class;
    }

    public String asString() {
        return "" + this.value;
    }
}