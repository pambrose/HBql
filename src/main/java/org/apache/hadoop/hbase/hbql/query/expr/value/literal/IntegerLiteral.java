package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.IntegerValue;

public class IntegerLiteral extends GenericLiteral implements IntegerValue {

    private final Integer value;

    public IntegerLiteral(final Integer value) {
        this.value = value;
    }

    public Integer getValue(final Object object) {
        return this.value;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return IntegerValue.class;
    }

    public String asString() {
        return "" + this.value;
    }
}