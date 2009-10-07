package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.LongValue;

public class LongLiteral extends GenericLiteral implements LongValue {

    private final Long value;

    public LongLiteral(final Long value) {
        this.value = value;
    }

    public Long getValue(final Object object) {
        return this.value;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return LongValue.class;
    }

    public String asString() {
        return "" + this.value;
    }
}