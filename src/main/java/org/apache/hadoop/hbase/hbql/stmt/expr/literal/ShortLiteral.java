package org.apache.hadoop.hbase.hbql.stmt.expr.literal;

import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.ShortValue;

public class ShortLiteral extends GenericLiteral<Short> implements ShortValue {

    public ShortLiteral(final Short value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return ShortValue.class;
    }
}